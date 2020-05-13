use async_std::net::TcpStream;
use async_std::net::ToSocketAddrs;
use anyhow::*;
use async_std::io::prelude::*;
use ws_stream_tungstenite::WsStream;
use async_io_stream::IoStream;
use async_tungstenite::WebSocketStream;
use async_tungstenite::stream::Stream;
use async_tls::client::TlsStream;
use futures_util::{SinkExt, AsyncReadExt};
use async_std::stream::StreamExt;
use futures::io::{WriteHalf, ReadHalf};
use async_std::io::Lines;

type SocketStream = WsStream<Stream<TcpStream, TlsStream<TcpStream>>>;

pub struct ReadSocket {
    read_stream: ReadHalf<SocketStream>
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
    Ok((ReadSocket{read_stream}, WriteSocket{write_stream}))
}

impl WriteSocket {
    pub async fn send<A : Into<Vec<u8>>>(&mut self, content: A) -> Result<()> {
        let mut vector = content.into();
        // encoder.apply_keystream(vector.as_mut_slice());
        self.write_stream.write_all(vector.as_slice()).await.map_err(|x|x.into())
    }
}

impl ReadSocket {
    pub async fn read_all(&mut self) {
        loop {
            let rd = async_std::io::BufReader::new(&mut self.read_stream);
            for i in rd.lines().next().await{
                if let Ok(i) = i {
                    println!("{}", i);
                }
            }
        }
    }
}
unsafe impl Send for ReadSocket {}
unsafe impl Send for WriteSocket {}

