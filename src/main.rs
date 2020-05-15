#![feature(type_ascription)]
#![feature(box_syntax)]

use async_std as astd;
use anyhow::*;
use log::*;
use structopt::StructOpt;
use xactor::Actor;
use systemstat::Duration;

mod database;
mod config;
mod socket;
mod status;
mod client;
mod trace;
#[global_allocator]
static GLOBAL : snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[async_std::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_custom_env("GIRASOL_LOG_LEVEL");
    let conf = config::Config::from_args();
    let db = database::init(&conf.home).await?;
    let db_actor = database::DataActor::new(db).start().await;
    let (mut rd, wt) = socket::create_sockets(
        "wss://megumi.yukipedia.cf:7443").await?;
    let send_client = client::SendClient::new (wt).start().await;
    rd.listen(db_actor.clone(), send_client.clone()).await;
    Ok(())
}


