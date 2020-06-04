use std::io::{BufRead, Write};
use std::process::Stdio;

use anyhow::*;
use hashbrown::HashMap;
use log::*;
use serde::*;
use systemstat::Duration;
use tempfile::NamedTempFile;
use typename::*;
use xactor::{Actor, Addr, Context, Handler, Message};

use crate::database::TraceModel;
use crate::utils::CheckError;

macro_rules! template {
    () => {
r#"
probe process("{}").function("{}").call {{
    printf("probe: %s", ppfunc());
    print_usyms(ucallers(-1));
}}
"#
};
}



fn to_script(function_list: &Vec<String>, process: &str, lasting: usize) -> String {
    let mut vec = function_list.iter()
        .map(|x| format!(template!(), process, x))
        .collect::<Vec<_>>();
    vec.push(format!("probe timer.s({}) {{exit(); }}\n", lasting));
    vec.join("\n")
}

fn to_tempfile(m: &TraceModel) -> Result<tempfile::NamedTempFile> {
    match &m.content {
        crate::database::TraceContent::SystemTap { function_list, process, .. } => {
            tempfile::NamedTempFile::new()
                .and_then(|mut x| x.write_all(to_script(function_list, process, m.lasting).as_bytes())
                    .map(|_| x))
                .map_err(|x| x.into())
        }
        crate::database::TraceContent::PerfBranch { .. } => {
            Err(anyhow!("perf based trace cannot be translated into temp files"))
        }
    }
}

pub struct HouseKeeper {
    pub(crate) send_client: Addr<crate::client::SendClient>,
    pub(crate) running_trace: HashMap<String, Addr<TraceActor>>,
}

pub struct TraceActor {
    house_keeper: Addr<HouseKeeper>,
    send_client: Addr<crate::client::SendClient>,
    model: TraceModel,
    file: Option<NamedTempFile>,
    child: Option<std::process::Child>,
}

#[xactor::message(result = "()")]
pub enum TraceEvent {
    NextRound,
    PerfEnding,
}

#[xactor::message(result = "()")]
pub enum KeeperMsg {
    Unregister(String),
    StartAll(Vec<TraceModel>),
    Start(TraceModel),
    StopAll,
}

#[xactor::message(result = "Vec<String>")]
pub struct AllRunning;

#[async_trait::async_trait]
impl Actor for TraceActor {
    async fn started(&mut self, ctx: &Context<Self>) {
        if let Err(e) = ctx.address().send(TraceEvent::NextRound) {
            error!("trace {} cannot start the event with err: {}, going to suicide!", self.model.name, e);
            ctx.stop(None);
        }
    }

    async fn stopped(&mut self, _: &Context<Self>) {
        if let Some(mut c) = self.child.take() {
            if let Err(e) = c.kill() {
                error!("cannot kill running perf {}, pid: {}", e, c.id())
            }
        }
        info!("trace {} actor stopped", self.model.name);
    }
}

impl TraceActor {
    async fn commit_suicide(&mut self) {
        self.house_keeper.send(KeeperMsg::Unregister(self.model.name.clone()))
            .map_err(|x| x.into())
            .check_error()
    }
}

#[xactor::message(result = "()")]
#[derive(Serialize, Deserialize, TypeName)]
pub struct Connect {
    trace_name: String,
    callee: String,
    caller: String,
    weight: usize,
}

#[xactor::message(result = "()")]
#[derive(Serialize, Deserialize, TypeName)]
pub struct TraceError {
    trace_name: String,
    content: String,
}

impl TraceActor {
    async fn handle_stap(&mut self, ctx: &Context<Self>) {
        match &self.model.content {
            crate::database::TraceContent::SystemTap {
                envs,
                args,
                ..
            } => {
                if self.file.is_none()
                {
                    match to_tempfile(&self.model) {
                        Ok(e) => { self.file.replace(e); }
                        Err(e) => {
                            error!("trace {} cannot create script file with error {}, going to suicide"
                                   , self.model.name, e);
                            self.commit_suicide().await;
                            return;
                        }
                    }
                }
                let file = self.file.as_ref().unwrap();
                match std::process::Command::new("stap")
                    .arg(file.path())
                    .envs(envs.clone().into_iter())
                    .args(args.iter())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map(|x| (x.stdout.unwrap(), x.stderr.unwrap())) {
                    Err(e) => {
                        error!("trace {} cannot create script file with error {}, going to suicide"
                               , self.model.name, e);
                        self.commit_suicide().await;
                        return;
                    }
                    Ok((out, err)) => {
                        let mut callee = None;
                        let err_name = self.model.name.clone();
                        let mut err_client = self.send_client.clone();
                        let err_handle = async_std::task::spawn(async move {
                            for i in std::io::BufReader::new(err).lines() {
                                if let Ok(c) = i {
                                    error!("trace {} error: {}", err_name, c);
                                    err_client.send(TraceError {
                                        trace_name: err_name.clone(),
                                        content: c,
                                    }).check_error();
                                }
                            }
                        });
                        for i in std::io::BufReader::new(out).lines() {
                            if let Ok(line) = i {
                                if let Some(t) = callee.take() {
                                    if line.contains(" : ") {
                                        let mut split = line.split(" : ");
                                        split.next();
                                        if let Some(e) = split.next()
                                            .and_then(|x| x.split("+")
                                                .next())
                                            .filter(|x| !x.starts_with("0x")) {
                                            self.send_client.send(Connect {
                                                trace_name: self.model.name.clone(),
                                                callee: t,
                                                caller: String::from(e),
                                                weight: 1,
                                            })
                                                .map_err(|x| x.into())
                                                .check_error()
                                        }
                                    }
                                } else {
                                    if line.starts_with("probe:") {
                                        let mut iter = line.split_ascii_whitespace();
                                        iter.next();
                                        callee = iter.next().map(|x| String::from(x))
                                    }
                                }
                            } else {
                                callee = None;
                            }
                        }
                        err_handle.await;
                    }
                }
                ctx.send_later(TraceEvent::NextRound, Duration::from_secs(self.model.interval as u64));
            }
            _ => unsafe { std::intrinsics::unreachable() }
        }
    }
    async fn handle_perf_ending(&mut self, ctx: &Context<Self>) {
        if let Some(mut child) = self.child.take() {
            child.kill().map_err(|x| x.into())
                .check_error();
            let filename = format!("/tmp/girasol-perf-{}.data", self.model.name);
            match std::process::Command::new("perf")
                .arg("report")
                .arg("-i")
                .arg(filename)
                .arg("-n")
                .arg("--sort")
                .arg("symbol_from,symbol_to")
                .arg("--stdio")
                .stdout(Stdio::piped())
                .spawn()
                .and_then(|x| x.wait_with_output())
                .map(|x| { x.stdout }) {
                Err(e) => {
                    self.send_client.send(TraceError {
                        trace_name: self.model.name.clone(),
                        content: e.to_string(),
                    }).check_error();
                }
                Ok(output) => {
                    let reader = output.lines();
                    for i in reader {
                        if let Ok(i) = i {
                            let res : &str = i.trim();
                            if !res.starts_with("#") && !res.is_empty() {
                                let mut words = res.split_ascii_whitespace();
                                words.next();
                                if let Some((count, from, to)) = words.next().and_then(
                                    |count| {
                                        words.next();
                                        words.next()
                                            .and_then(|from| {
                                                words.next();
                                                words.next().map(|to|
                                                    (count, from, to)
                                                )
                                            })
                                    }
                                ) {
                                    let count: usize = count.parse().unwrap_or(0);
                                    self.send_client.send(Connect {
                                        trace_name: self.model.name.clone(),
                                        callee: to.to_string(),
                                        caller: from.to_string(),
                                        weight: count
                                    }).check_error();
                                }
                            }
                        }
                    }
                }
            }
        }
        ctx.send_later(TraceEvent::NextRound, Duration::from_secs(self.model.interval as u64))
    }
    async fn handle_perf(&mut self, ctx: &Context<Self>) {
        match &self.model.content {
            crate::database::TraceContent::PerfBranch {
                frequency, absolute_path, additional_args, ..
            } => {
                match crate::utils::find_running(absolute_path.as_str())
                    .map(|x| x.into_iter().map(|x| x.to_string()))
                    .map(|x| x.collect::<Vec<_>>().join(",")) {
                    Ok(pids) => {
                        let mut child = std::process::Command::new("perf");
                        child.arg("record")
                            .arg("--branch-filter=any_call,u")
                            .arg("-e")
                            .arg("branches:u")
                            .arg("-p")
                            .arg(pids)
                            .arg("-o")
                            .arg(format!("/tmp/girasol-perf-{}.data", self.model.name))
                            .args(additional_args.iter())
                            .stderr(Stdio::piped());
                        match frequency {
                            crate::database::Frequency::Max => {
                                child.arg("-Fmax");
                            }
                            crate::database::Frequency::Specific(value) => {
                                child.arg("-F").arg(value.to_string());
                            }
                            _ => ()
                        }
                        match child.spawn() {
                            Ok(mut c) => {
                                {
                                    let mut addr = self.send_client.clone();
                                    let stderr = c.stderr.take().unwrap();
                                    let name = self.model.name.clone();
                                    async_std::task::spawn(async move {
                                        for i in std::io::BufReader::new(stderr).lines() {
                                            if let Ok(line) = i {
                                                addr.send(TraceError {
                                                    trace_name: name.clone(),
                                                    content: line,
                                                }).check_error();
                                            }
                                        }
                                    });
                                }
                                self.child.replace(c);
                                info!("perf started");
                                ctx.send_later(TraceEvent::PerfEnding, Duration::from_secs(self.model.lasting as u64))
                            }
                            Err(e) => {
                                self.send_client.send(TraceError {
                                    trace_name: self.model.name.clone(),
                                    content: e.to_string(),
                                }).check_error();
                            }
                        }
                    }
                    Err(e) => {
                        self.send_client.send(TraceError {
                            trace_name: self.model.name.clone(),
                            content: e.to_string(),
                        }).check_error();
                    }
                }
            }
            _ => unsafe { std::intrinsics::unreachable() }
        }
    }
}

#[async_trait::async_trait]
impl Handler<TraceEvent> for TraceActor {
    async fn handle(&mut self, ctx: &Context<Self>, event: TraceEvent) {
        match event {
            TraceEvent::NextRound => match self.model.content {
                crate::database::TraceContent::SystemTap {
                    ..
                } => {
                    self.handle_stap(ctx).await
                }
                crate::database::TraceContent::PerfBranch {
                    ..
                } => {
                    self.handle_perf(ctx).await
                }
            }
            TraceEvent::PerfEnding => self.handle_perf_ending(ctx).await
        }
    }
}

impl HouseKeeper {
    async fn create_actor(&mut self, model: TraceModel, ctx: &Context<Self>) -> Result<()> {
        let flag = self.running_trace.contains_key(model.name.as_str());
        if !flag {
            let name = model.name.clone();
            let actor = TraceActor {
                house_keeper: ctx.address(),
                send_client: self.send_client.clone(),
                model,
                file: None,
                child: None,
            };
            let addr = actor.start().await;
            self.running_trace.insert(name, addr);
            Ok(())
        } else {
            Err(anyhow!("{} already running", model.name))
        }
    }
}

#[async_trait::async_trait]
impl Actor for HouseKeeper {
    async fn started(&mut self, _: &Context<Self>) {
        info!("house keeper started");
    }
}

#[async_trait::async_trait]
impl Handler<KeeperMsg> for HouseKeeper {
    async fn handle(&mut self, ctx: &Context<Self>, msg: KeeperMsg) -> <KeeperMsg as Message>::Result {
        match msg {
            KeeperMsg::Unregister(name) =>
                {
                    for mut i in self.running_trace.remove(name.as_str()) {
                        i.stop(None).check_error();
                        info!("send stop to trace {} at {}", name, i.actor_id());
                    }
                }
            KeeperMsg::StartAll(list) => {
                for i in list {
                    if !self.running_trace.contains_key(i.name.as_str()) {
                        self.create_actor(i, ctx).await.check_error();
                    }
                }
            }
            KeeperMsg::Start(model) => {
                if !self.running_trace.contains_key(model.name.as_str()) {
                    self.create_actor(model, ctx).await.check_error();
                }
            }
            KeeperMsg::StopAll => {
                for (name, addr) in self.running_trace.iter_mut() {
                    addr.stop(None).check_error();
                    info!("send stop to trace {} at {}", name, addr.actor_id());
                }
                self.running_trace.clear();
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<AllRunning> for HouseKeeper {
    async fn handle(&mut self, _: &Context<Self>, _: AllRunning) -> <AllRunning as Message>::Result {
        self.running_trace.keys().map(|x| x.clone()).collect()
    }
}