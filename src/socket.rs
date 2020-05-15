use anyhow::*;
use async_std::io::prelude::*;
use async_std::net::TcpStream;
use async_std::stream::StreamExt;
use async_tls::client::TlsStream;
use async_tungstenite::stream::Stream;
use futures::io::{ReadHalf, WriteHalf};
use log::*;
use crate::utils::CheckError;
use ws_stream_tungstenite::WsStream;
use xactor::Addr;

use crate::database::{DbMsg, DbReply, TraceModel};

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
}

#[xactor::message(result = "()")]
#[derive(typename::TypeName, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum ClientReply {
    QueryResult(TraceModel),
    QueryList(Vec<TraceModel>),
    Error(String),
    Success(String),
}




impl ReadSocket {
    pub async fn listen(&mut self, db: Addr<crate::database::DataActor>,
                        client: Addr<crate::client::SendClient>) {
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
                        },
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
                                                    _ => unsafe {std::intrinsics::unreachable(); }
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
                                                    _ => unsafe {std::intrinsics::unreachable(); }
                                                }
                                            }
                                        }
                                    });
                                    debug!("query issued at task {}", handle.task().id())
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
                                            _ => unsafe {std::intrinsics::unreachable(); }
                                        }
                                    });
                                    debug!("add issued at task{}", handle.task().id())
                                }
                                ServerMsg::Remove(name) => {
                                    let mut client = client.clone();
                                    let mut db = db.clone();
                                    let handle = async_std::task::spawn(async move {
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
                                            _ => unsafe {std::intrinsics::unreachable(); }
                                        }
                                    });
                                    debug!("add issued at task {}", handle.task().id())
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

