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



fn to_script(m: &TraceModel) -> String {
    let mut vec = m.function_list.iter()
        .map(|x| format!(template!(), m.process, x))
        .collect::<Vec<_>>();
    vec.push(format!("probe timer.s({}) {{exit(); }}\n", m.lasting));
    vec.join("\n")
}

fn to_tempfile(m: &TraceModel) -> Result<tempfile::NamedTempFile> {
    tempfile::NamedTempFile::new()
        .and_then(|mut x| x.write_all(to_script(m).as_bytes())
            .map(|_| x))
        .map_err(|x| x.into())
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
}

#[xactor::message(result = "()")]
struct NextRound;

#[xactor::message(result = "()")]
pub enum KeeperMsg {
    Unregister(String),
    StartAll(Vec<TraceModel>),
    Start(TraceModel),
}

#[xactor::message(result = "Vec<String>")]
pub struct AllRunning;

#[async_trait::async_trait]
impl Actor for TraceActor {
    async fn started(&mut self, ctx: &Context<Self>) {
        if let Err(e) = ctx.address().send(NextRound) {
            error!("trace {} cannot start the event with err: {}, going to suicide!", self.model.name, e);
            ctx.stop(None);
        }
    }
    async fn stopped(&mut self, _: &Context<Self>) {
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
}

#[xactor::message(result = "()")]
#[derive(Serialize, Deserialize, TypeName)]
pub struct TraceError {
    trace_name: String,
    content: String,
}

#[async_trait::async_trait]
impl Handler<NextRound> for TraceActor {
    async fn handle(&mut self, ctx: &Context<Self>, _: NextRound) -> <NextRound as Message>::Result {
        if self.file.is_none() {
            match to_tempfile(&self.model) {
                Ok(e) => { self.file.replace(e); }
                Err(e) => {
                    error!("trace {} cannot create script file with error {}, going to suicide"
                           , self.model.name, e);
                    self.commit_suicide().await;
                }
            }
        }
        let file = self.file.as_ref().unwrap();
        match std::process::Command::new("stap")
            .arg(file.path())
            .envs(self.model.envs.clone().into_iter())
            .args(self.model.args.iter())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map(|x| (x.stdout.unwrap(), x.stderr.unwrap())) {
            Err(e) => {
                error!("trace {} cannot create script file with error {}, going to suicide"
                       , self.model.name, e);
                self.commit_suicide().await;
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
        ctx.send_later(NextRound, Duration::from_secs(self.model.interval as u64));
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
        }
    }
}

#[async_trait::async_trait]
impl Handler<AllRunning> for HouseKeeper {
    async fn handle(&mut self, _: &Context<Self>, _: AllRunning) -> <AllRunning as Message>::Result {
        self.running_trace.keys().map(|x|x.clone()).collect()
    }
}