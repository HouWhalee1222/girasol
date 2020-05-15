use log::*;
use systemstat::Duration;
use xactor::*;

use crate::socket::{WriteSocket, ClientReply};
use crate::status::HeartbeatPacket;
use serde::Serialize;
use typename::TypeName;

pub struct SendClient {
    pub(crate) socket: WriteSocket
}

macro_rules! msg_template {
    () => {r#"{{"type": "{}", "content": {}}}"#};
}

impl SendClient {
    pub fn new(socket: WriteSocket) -> Self {
        SendClient {
            socket
        }
    }

    async fn send_json<T : Serialize + TypeName>(&mut self, data: T) -> anyhow::Result<()> {
        let data = simd_json::to_string(&data)?;
        self.socket.send(format!(msg_template!(), T::type_name(), data)).await
    }

    async fn send_json_directly<T : Serialize>(&mut self, data: T) -> anyhow::Result<()> {
        let data = simd_json::to_string(&data)?;
        self.socket.send(data).await
    }
}

#[async_trait::async_trait]
impl Actor for SendClient {
    async fn started(&mut self, ctx: &Context<Self>) {
        info!("send client started");
        ctx.send_interval_with(crate::status::get_status, Duration::from_secs(1))
    }
}

#[async_trait::async_trait]
impl Handler<ClientReply> for SendClient {
    async fn handle(&mut self, _: &Context<Self>, msg: ClientReply) -> <ClientReply as Message>::Result {
        match self.send_json_directly(msg).await {
            Err(e) => error!("{}", e),
            Ok(_) => debug!("client reply data sent successfully")
        }
    }
}

#[async_trait::async_trait]
impl<T : typename::TypeName + Serialize + Message<Result = ()>> Handler<T> for SendClient {
    async fn handle(&mut self, _: &Context<Self>, msg: T) -> <T as Message>::Result {
        match self.send_json(msg).await {
            Err(e) => error!("{}", e),
            Ok(_) => debug!("{} data sent successfully", T::type_name())
        }
    }
}



