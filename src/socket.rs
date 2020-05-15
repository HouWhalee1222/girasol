use anyhow::*;
use async_io_stream::IoStream;
use async_std::io::Lines;
use async_std::io::prelude::*;
use async_std::net::TcpStream;
use async_std::net::ToSocketAddrs;
use async_std::stream::StreamExt;
use async_tls::client::TlsStream;
use async_tungstenite::stream::Stream;
use async_tungstenite::WebSocketStream;
use futures::io::{ReadHalf, WriteHalf};
use futures_util::{AsyncReadExt, SinkExt};
use log::*;
use systemstat::Duration;
use ws_stream_tungstenite::WsStream;
use crate::database::{TraceModel, DbMsg, DbReply};
use xactor::Addr;

type SocketStream = WsStream<Stream<TcpStream, TlsStream<TcpStream>>>;

pub struct ReadSocket {
    read_stream: Option<ReadHalf<SocketStream>>
}

pub struct WriteSocket {
    write_stream: WriteHalf<SocketStream>
}

pub async fn create_sockets(server: &str) -> Result<(ReadSocket, WriteSocket)> {
    let (wstream, resp) =
        async_tungstenite::async_std::connect_async(server)
            .await?;
    let (read_stream, write_stream) =
        futures_util::io::AsyncReadExt::split(WsStream::new(wstream));
    Ok((ReadSocket { read_stream: Some(read_stream) }, WriteSocket { write_stream }))
}

impl WriteSocket {
    pub async fn send<A: Into<Vec<u8>>>(&mut self, content: A) -> Result<()> {
        let mut vector = content.into();
        // encoder.apply_keystream(vector.as_mut_slice());
        self.write_stream.write_all(vector.as_slice()).await.map_err(|x| x.into())
    }
}

#[derive(typename::TypeName, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum ServerMsg {
    Reply(String),
    QueryAll,
    Query(String)
}

#[xactor::message(result="()")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum ClientReply {
    QueryResult(TraceModel),
    QueryList(Vec<TraceModel>),
    Error(String)
}

trait CheckError {
    fn check_error(&self);
}

impl<T> CheckError for anyhow::Result<T> {
    fn check_error(&self) {
        if let Err(e) = self {
            error!("{}", e)
        }
    }
}


impl ReadSocket {
    pub async fn listen(&mut self, mut db: Addr<crate::database::DataActor>,
                        mut client : Addr<crate::client::SendClient>)  {
        let stream = self.read_stream.take().unwrap();
        let mut reader = async_std::io::BufReader::new(stream)
            .lines();
        while let Some(t) = reader.next().await {
            match t {
                Ok(mut t) => {
                    debug!("incomming request: {}", t);
                    match simd_json::from_str::<ServerMsg>(t.as_mut_str()) {
                        Err(e) => error!("failed to parse server message: {}", e),
                        Ok(msg) => {
                            match msg {
                                ServerMsg::Reply(msg) => {
                                    debug!("server replied {} for handshake", msg)
                                },
                                ServerMsg::Query(msg) => {
                                    let mut client = client.clone();
                                    let mut db = db.clone();
                                    let handle = async_std::task::spawn(async move {
                                        match db.call(DbMsg::Get(msg)).await
                                            .map_err(|x|x.into())
                                            .and_then(|x|x) {
                                            Err(e) => {
                                                error!("{}", e);
                                                client.send(ClientReply::Error(e.to_string()))
                                                    .check_error();
                                            },
                                            Ok(t) => {
                                                match t {
                                                    DbReply::GetResult(t) =>
                                                        {client.send(ClientReply::QueryResult(t))
                                                            .check_error(); }
                                                    _ => error!("actor replied unexpected message")
                                                }
                                            }
                                        }
                                    });
                                    debug!("query issue at tast {}", handle.task().id())
                                }
                                _ => unimplemented!()
                            }
                        }
                    }

                }
                Err(e) => {
                    error!("communication error: {}", e);
                }
            }
        }
    }
}

unsafe impl Send for ReadSocket {}

unsafe impl Send for WriteSocket {}

