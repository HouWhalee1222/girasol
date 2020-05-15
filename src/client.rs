use log::*;
use systemstat::Duration;
use xactor::*;

use crate::socket::WriteSocket;
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
}

#[async_trait::async_trait]
impl Actor for SendClient {
    async fn started(&mut self, ctx: &Context<Self>) {
        info!("send client started");
        ctx.send_interval_with(crate::status::get_status, Duration::from_secs(5))
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



