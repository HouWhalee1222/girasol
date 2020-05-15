use std::path::{Path, PathBuf};

use anyhow::*;
use async_std as astd;
use futures::future::*;
use futures_util::*;
use log::*;
use serde::Serialize;
use xactor::{Actor, Handler, Message};

pub async fn init<A: AsRef<Path>>(home: A) -> Result<sled::Db> {
    sled::open(home.as_ref().join("database"))
        .map_err(|x| x.into())
}

pub async fn query_str<A: AsRef<str>>(key: A, db: &sled::Db) -> Result<String> {
    db.get(key.as_ref())
        .map_err(|x| x.into())
        .and_then(|x|
            x.ok_or(Error::msg(format!("key {} not set", key.as_ref()))))
        .and_then(|x| String::from_utf8(x.to_vec())
            .map_err(|x| x.into()))
}

pub async fn query_json<K, T>(key: K, db: &sled::Db) -> Result<T>
    where for<'de>
          T: serde::Deserialize<'de>,
          K: AsRef<str> {
    db.get(key.as_ref())
        .map_err(|x| x.into())
        .and_then(|x|
            x.ok_or(Error::msg(format!("key {} not set", key.as_ref()))))
        .and_then(|x| {
            let mut v = x.to_vec();
            simd_json::serde::from_slice(v.as_mut_slice())
                .map_err(|x| x.into())
        })
}

pub async fn insert_str<A: AsRef<str>, B: AsRef<str>>(key: A, content: B, db: &sled::Db)
                                                      -> Result<()> {
    match db.contains_key(key.as_ref())
        .map_err(|e| e.into())
        .and_then(|flag| if flag { Err(anyhow!("{} exists", key.as_ref())) } else { Ok(()) })
        .and_then(|_| db.insert(key.as_ref(), content.as_ref()).map_err(|x| x.into())) {
        Ok(_) => db.flush_async().await.map(|e| trace!("{} bytes flushed", e)).map_err(|x| x.into()),
        e => e.map(|_| ())
    }
}

pub async fn insert_obj<A: AsRef<str>, B: Serialize>(key: A, content: B, db: &sled::Db)
                                                     -> Result<()> {
    match db.contains_key(key.as_ref())
        .map_err(|e| e.into())
        .and_then(|flag| if flag { Err(anyhow!("{} exists", key.as_ref())) } else { Ok(()) })
        .and_then(|_| simd_json::to_vec(&content).map_err(|x| x.into()))
        .and_then(|obj| db.insert(key.as_ref(), obj).map_err(|x| x.into())) {
        Ok(_) => db.flush_async().await.map(|e| trace!("{} bytes flushed", e)).map_err(|x| x.into()),
        e => e.map(|_| ())
    }
}

pub struct DataActor {
    db: sled::Db
}

impl DataActor {
    pub fn new(db: sled::Db) -> Self {
        DataActor {
            db
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum Tracer {
    EBPF,
    STAP,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TraceModel {
    name: String,
    function_list: Vec<String>,
    process: PathBuf,
    tracer: Tracer,
    args: Vec<String>,
    env: Vec<String>,
    lasting: usize,
    interval: usize,
}

#[xactor::message(result = "anyhow::Result<DbReply>")]
pub enum DbMsg {
    QueryAll,
    Get(String),
    Remove(String),
    Add(TraceModel),
}

pub enum DbReply {
    AllList(Vec<TraceModel>),
    GetResult(TraceModel),
    Success,
}

#[async_trait::async_trait]
impl Actor for DataActor {
    async fn started(&mut self, _: &xactor::Context<Self>) {
        info!("database actor started");
    }
    async fn stopped(&mut self, _: &xactor::Context<Self>) {
        if let Err(e) = self.db.flush_async().await
            .map(|num| trace!("database finalized with {} bytes", num)) {
            error!("{}", e);
        }
    }
}

#[async_trait::async_trait]
impl Handler<DbMsg> for DataActor {
    async fn handle(&mut self, _ctx: &xactor::Context<Self>, msg: DbMsg) -> <DbMsg as Message>::Result {
        match msg {
            DbMsg::QueryAll => {
                let mut result = Ok(Vec::new());
                for i in self.db.iter() {
                    result = result.and_then(|mut x| {
                        i.map_err(|x| x.into())
                            .map(|(_, y)| y)
                            .map(|x| x.to_vec())
                            .and_then(|mut x| simd_json::from_slice(x.as_mut_slice())
                                .map_err(|x|x.into()))
                            .map(|model| {
                                x.push(model);
                                x
                            })
                    })
                }
                result.map(|x| DbReply::AllList(x))
            }
            DbMsg::Get(name) => {
                query_json(name, &self.db).await
                    .map(|x| DbReply::GetResult(x))
            }
            DbMsg::Remove(name) => {
                match self.db.contains_key(&name) {
                    Ok(true) => self.db.remove(name).map(|_| DbReply::Success)
                        .map_err(|x| x.into()),
                    Ok(false) => Err(anyhow!("{} does not exist", name)),
                    Err(e) => Err(e.into())
                }
            }
            DbMsg::Add(model) => {
                match self.db.contains_key(&model.name) {
                    Ok(true) => Err(anyhow!("{} exists", model.name)),
                    Ok(false) => insert_obj(model.name.clone(), model, &self.db).await
                        .map(|_| DbReply::Success),
                    Err(e) => Err(e.into())
                }
            }
        }
    }
}
