use std::io::{Write, BufRead};
use std::process::Stdio;

use anyhow::*;
use serde::*;
use typename::*;
use async_std::sync::Arc;
use hashbrown::HashMap;
use log::*;
use systemstat::Duration;
use tempfile::NamedTempFile;
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
    send_client: Addr<crate::client::SendClient>,
    running_trace: HashMap<String, Addr<TraceActor>>,
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
struct Unregister(String);

#[async_trait::async_trait]
impl Actor for TraceActor {
    async fn started(&mut self, ctx: &Context<Self>) {
        if let Err(e) = ctx.address().send(NextRound) {
            error!("trace {} cannot start the event, going to suicide!", self.model.name);
            ctx.stop(None);
        }
    }
    async fn stopped(&mut self, _: &Context<Self>) {
        info!("trace {} actor stopped", self.model.name);
        self.house_keeper.send(Unregister(self.model.name.clone()))
            .map_err(|x|x.into())
            .check_error()
    }
}
#[xactor::message(result = "()")]
#[derive(Serialize, Deserialize, TypeName)]
pub struct Connect {
    trace_name: String,
    callee: String,
    caller: String
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
                    ctx.stop(None);
                }
            }
        }
        let file = self.file.as_ref().unwrap();
        match std::process::Command::new("stap")
            .arg(file.path())
            .envs(self.model.envs.clone().into_iter())
            .args(self.model.args.iter())
            .stdout(Stdio::piped())
            .spawn()
            .map(|mut x| x.stdout.unwrap()) {
            Err(e) => {
                error!("trace {} cannot create script file with error {}, going to suicide"
                       , self.model.name, e);
                ctx.stop(None);
            },
            Ok(out) => {
                let mut callee = None;
                for i in std::io::BufReader::new(out).lines() {
                    if let Ok(line) = i {
                        if let Some(t) = callee.take() {
                            if line.contains(" : ") {
                                let mut split = line.split(" : ");
                                split.next();
                                if let Some(e) = split.next()
                                    .and_then(|x|x.split("+")
                                        .next())
                                    .filter(|x| !x.starts_with("0x")){
                                       self.send_client.send(Connect {
                                           trace_name: self.model.name.clone(),
                                           callee: t,
                                           caller: String::from(e)
                                       })
                                           .map_err(|x|x.into())
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
            }
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
                file: None
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
impl Handler<Unregister> for HouseKeeper {
    async fn handle(&mut self, ctx: &Context<Self>, msg: Unregister) -> <Unregister as Message>::Result {
        self.running_trace.remove(msg.0.as_str());
    }
}