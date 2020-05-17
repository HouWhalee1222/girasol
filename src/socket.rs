use anyhow::*;
use async_std::io::prelude::*;
use async_std::net::TcpStream;
use async_std::stream::StreamExt;
use async_tls::client::TlsStream;
use async_tungstenite::stream::Stream;
use futures::io::{ReadHalf, WriteHalf};
use log::*;
use ws_stream_tungstenite::WsStream;
use xactor::Addr;

use crate::database::{DbMsg, DbReply, TraceModel};
use crate::trace::KeeperMsg;
use crate::utils::CheckError;

type SocketStream = WsStream<Stream<TcpStream, TlsStream<TcpStream>>>;

pub struct ReadSocket {
    read_stream: Option<ReadHalf<SocketStream>>
}

pub struct WriteSocket {
    write_stream: WriteHalf<SocketStream>
}

pub async fn create_sockets(server: &str) -> Result<(ReadSocket, WriteSocket)> {
    let (wstream, _resp) =
        async_tungstenite::async_std::connect_async(server)
            .await?;
    let (read_stream, write_stream) =
        futures_util::io::AsyncReadExt::split(WsStream::new(wstream));
    Ok((ReadSocket { read_stream: Some(read_stream) }, WriteSocket { write_stream }))
}

impl WriteSocket {
    pub async fn send<A: Into<Vec<u8>>>(&mut self, content: A) -> Result<()> {
        let vector = content.into();
        // encoder.apply_keystream(vector.as_mut_slice());
        self.write_stream.write_all(vector.as_slice()).await.map_err(|x| x.into())
    }
}

#[derive(typename::TypeName, serde::Serialize, serde::Deserialize)]
#[serde(tag = "tag", content = "content")]
pub enum ServerMsg {
    Reply(String),
    QueryAll,
    Query(String),
    Add(TraceModel),
    Remove(String),
    Start(String),
    Stop(String),
    StartAll,
    QueryRunning,
    StopAll
}

#[xactor::message(result = "()")]
#[derive(typename::TypeName, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum ClientReply {
    QueryResult(TraceModel),
    QueryList(Vec<TraceModel>),
    Error(String),
    Success(String),
    Running(Vec<String>),
}


impl ReadSocket {
    pub async fn listen(&mut self, db: Addr<crate::database::DataActor>,
                        client: Addr<crate::client::SendClient>,
                        keeper: Addr<crate::trace::HouseKeeper>) {
        info!("start listening server event");
        let stream = self.read_stream.take().unwrap();
        let mut reader = async_std::io::BufReader::new(stream)
            .lines();
        while let Some(t) = reader.next().await {
            match t {
                Ok(mut t) => {
                    debug!("incomming request: {}", t);
                    match simd_json::from_str::<ServerMsg>(t.as_mut_str()) {
                        Err(e) => {
                            error!("failed to parse server message: {}", e);
                            let mut client = client.clone();
                            let handle = async_std::task::spawn(async move {
                                client.send(ClientReply::Error(e.to_string()))
                                    .check_error();
                            });
                            debug!("reply error to server at task {}", handle.task().id())
                        }
                        Ok(msg) => {
                            match msg {
                                ServerMsg::Reply(msg) => {
                                    debug!("server replied {} for handshake", msg)
                                }
                                ServerMsg::Query(msg) => {
                                    let mut client = client.clone();
                                    let mut db = db.clone();
                                    let handle = async_std::task::spawn(async move {
                                        match db.call(DbMsg::Get(msg)).await
                                            .map_err(|x| x.into())
                                            .and_then(|x| x) {
                                            Err(e) => {
                                                error!("{}", e);
                                                client.send(ClientReply::Error(e.to_string()))
                                                    .check_error();
                                            }
                                            Ok(t) => {
                                                match t {
                                                    DbReply::GetResult(t) =>
                                                        {
                                                            client.send(ClientReply::QueryResult(t))
                                                                .check_error();
                                                        }
                                                    _ => unsafe { std::intrinsics::unreachable(); }
                                                }
                                            }
                                        }
                                    });
                                    debug!("query issued at task {}", handle.task().id())
                                }
                                ServerMsg::QueryAll => {
                                    let mut client = client.clone();
                                    let mut db = db.clone();
                                    let handle = async_std::task::spawn(async move {
                                        match db.call(DbMsg::QueryAll).await
                                            .map_err(|x| x.into())
                                            .and_then(|x| x) {
                                            Err(e) => {
                                                error!("{}", e);
                                                client.send(ClientReply::Error(e.to_string()))
                                                    .check_error();
                                            }
                                            Ok(t) => {
                                                match t {
                                                    DbReply::AllList(t) =>
                                                        {
                                                            client.send(ClientReply::QueryList(t))
                                                                .check_error();
                                                        }
                                                    _ => unsafe { std::intrinsics::unreachable(); }
                                                }
                                            }
                                        }
                                    });
                                    debug!("query all issued at task {}", handle.task().id())
                                }
                                ServerMsg::Add(model) => {
                                    let mut client = client.clone();
                                    let mut db = db.clone();
                                    let name = model.name.clone();
                                    let handle = async_std::task::spawn(async move {
                                        match db.call(DbMsg::Add(model)).await
                                            .map_err(|x| x.into())
                                            .and_then(|x| x) {
                                            Err(e) => {
                                                error!("{}", e);
                                                client.send(ClientReply::Error(e.to_string()))
                                                    .check_error();
                                            }
                                            Ok(DbReply::Success) => {
                                                client.send(ClientReply::Success(format!("{} added", name)))
                                                    .check_error();
                                            }
                                            _ => unsafe { std::intrinsics::unreachable(); }
                                        }
                                    });
                                    debug!("add issued at task{}", handle.task().id())
                                }
                                ServerMsg::Remove(name) => {
                                    let mut client = client.clone();
                                    let mut db = db.clone();
                                    let mut keeper = keeper.clone();
                                    let handle = async_std::task::spawn(async move {
                                        keeper.send(KeeperMsg::Unregister(name.clone()))
                                            .check_error();
                                        match db.call(DbMsg::Remove(name.clone())).await
                                            .map_err(|x| x.into())
                                            .and_then(|x| x) {
                                            Err(e) => {
                                                error!("{}", e);
                                                client.send(ClientReply::Error(e.to_string()))
                                                    .check_error();
                                            }
                                            Ok(DbReply::Success) => {
                                                client.send(ClientReply::Success(format!("{} removed", name)))
                                                    .check_error();
                                            }
                                            _ => unsafe { std::intrinsics::unreachable(); }
                                        }
                                    });
                                    debug!("remove issued at task {}", handle.task().id())
                                }
                                ServerMsg::QueryRunning => {
                                    let mut keeper = keeper.clone();
                                    let mut client = client.clone();
                                    let handle = async_std::task::spawn(async move {
                                        match keeper.call(crate::trace::AllRunning)
                                            .await {
                                            Ok(list) => client.send(ClientReply::Running(list)).check_error(),
                                            Err(e) => {
                                                error!("{}", e);
                                                client.send(ClientReply::Error(format!("failed to get runing list: {}", e)))
                                                    .check_error();
                                            }
                                        }
                                    });
                                    debug!("running list query issued at task {}", handle.task().id())
                                }
                                ServerMsg::Stop(name) => {
                                    let mut keeper = keeper.clone();
                                    let mut client = client.clone();
                                    let handle = async_std::task::spawn(async move {
                                        match keeper.call(KeeperMsg::Unregister(name.clone()))
                                            .await {
                                            Err(e) => {
                                                error!("{}", e);
                                                client.send(ClientReply::Error(format!("failed to get runing list: {}", e)))
                                                    .check_error();
                                            }
                                            Ok(_) => client.send(ClientReply::Success(format!("stop trace {}", name)))
                                                .check_error(),
                                        }
                                    });
                                    debug!("stop trace issued at task {}", handle.task().id())
                                }
                                ServerMsg::StopAll => {
                                    let mut client = client.clone();
                                    let mut keeper = keeper.clone();
                                    let handle = async_std::task::spawn(async move {
                                        match keeper.send(KeeperMsg::StopAll) {
                                            Ok(_) => client.send(ClientReply::Success(String::from(
                                                "stopped all traces"
                                            ))).check_error(),
                                            Err(e) => client.send(ClientReply::Error(format!(
                                                "cannot issue stop event: {}", e
                                            ))).check_error()
                                        }
                                    });
                                    debug!("stop all trace issued at task {}", handle.task().id())
                                }
                                ServerMsg::Start(name) => {
                                    let mut client = client.clone();
                                    let mut db = db.clone();
                                    let mut keeper = keeper.clone();
                                    let handle = async_std::task::spawn(async move {
                                        match db.call(DbMsg::Get(name.clone()))
                                            .await
                                            .map_err(|x|x.into())
                                            .and_then(|x|x){
                                            Err(e) => {
                                                error!("{}", e);
                                                client.send(ClientReply::Error(e.to_string()))
                                                    .check_error();
                                            }
                                            Ok(t) => {
                                                match t {
                                                    DbReply::GetResult(t) =>
                                                        {
                                                            match keeper.call(KeeperMsg::Start(t))
                                                                .await {
                                                                Err(e) => {
                                                                    error!("{}", e);
                                                                    client.send(ClientReply::Error(format!("failed to start trace {}: {}", name, e)))
                                                                        .check_error();
                                                                }
                                                                Ok(_) => client.send(ClientReply::Success(format!("start trace {}", name)))
                                                                    .check_error(),
                                                            }
                                                        }
                                                    _ => unsafe { std::intrinsics::unreachable(); }
                                                }
                                            }
                                        }
                                    });
                                    debug!("start trace issued at task {}", handle.task().id())
                                }
                                ServerMsg::StartAll => {
                                    let mut client = client.clone();
                                    let mut db = db.clone();
                                    let mut keeper = keeper.clone();
                                    let handle = async_std::task::spawn(async move {
                                        match db.call(DbMsg::QueryAll).await
                                            .map_err(|x| x.into())
                                            .and_then(|x| x) {
                                            Err(e) => {
                                                error!("{}", e);
                                                client.send(ClientReply::Error(e.to_string()))
                                                    .check_error();
                                            }
                                            Ok(t) => {
                                                match t {
                                                    DbReply::AllList(t) =>
                                                        {
                                                            match keeper.call(KeeperMsg::StartAll(t))
                                                                .await {
                                                                Err(e) => {
                                                                    error!("{}", e);
                                                                    client.send(ClientReply::Error(format!("failed to start all traces: {}", e)))
                                                                        .check_error();
                                                                }
                                                                Ok(_) => client.send(ClientReply::Success(format!("start all traces")))
                                                                    .check_error(),
                                                            }
                                                        }
                                                    _ => unsafe { std::intrinsics::unreachable(); }
                                                }
                                            }
                                        }
                                    });
                                    debug!("start all issued at task {}", handle.task().id())
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("communication error: {}", e);
                    let mut client = client.clone();
                    let handle = async_std::task::spawn(async move {
                        client.send(ClientReply::Error(e.to_string()))
                            .check_error();
                    });
                    debug!("reply error to server at task {}", handle.task().id())
                }
            }
        }
    }
}

unsafe impl Send for ReadSocket {}

unsafe impl Send for WriteSocket {}

