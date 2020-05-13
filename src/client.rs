use crate::socket::WriteSocket;
use xactor::*;
use log::*;
use systemstat::Duration;
use crate::status::HeartbeatPacket;

pub struct SendClient {
    pub(crate) socket: WriteSocket
}

#[async_trait::async_trait]
impl Actor for SendClient{
    async fn started(&mut self, ctx: &Context<Self>) {
        ctx.send_interval_with(crate::status::get_status, Duration::from_secs(1))
    }
}

#[async_trait::async_trait]
impl Handler<HeartbeatPacket> for SendClient {
    async fn handle(&mut self, _: &Context<Self>, msg: HeartbeatPacket) -> <HeartbeatPacket as Message>::Result {
        match simd_json::to_vec(&msg) {
            Err(e) => error!("heartbeat failed {}", e),
            Ok(data) => if let Err(e) = self.socket.send(data).await {
                error!("heartbeat failed {}", e)
            }
        }
    }
}

