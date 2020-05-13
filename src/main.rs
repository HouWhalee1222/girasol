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
#[global_allocator]
static GLOBAL : snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;



#[async_std::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_custom_env("GIRASOL_LOG_LEVEL");
    let conf = config::Config::from_args();
    let db = database::init(&conf.home).await?;
    let (rd, wt) = socket::create_sockets(
        "wss://hiv-arising-shift-decade.trycloudflare.com").await?;

    client::SendClient {socket: wt}.start().await;
    async_std::task::sleep(Duration::from_secs(30)).await;
    db.flush_async().await
        .map(|num| trace!("database finalized with {} bytes", num))
        .map_err(|x|x.into())
}
