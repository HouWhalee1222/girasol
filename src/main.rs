#![feature(type_ascription)]
#![feature(box_syntax)]
#![feature(core_intrinsics)]
use anyhow::*;
use structopt::StructOpt;
use xactor::Actor;

use config::{Config, SubCommand};

mod database;
mod config;
mod socket;
mod status;
mod client;
mod trace;
mod utils;
#[global_allocator]
static GLOBAL: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[async_std::main]
async fn main() -> Result<()> {
    pretty_env_logger::try_init_timed_custom_env("GIRASOL_LOG_LEVEL")?;
    let conf: Config = config::Config::from_args();
    let db = database::init(&conf.home).await?;
    let mut db_actor = database::DataActor::new(db).start().await;
    match conf.subcommand {
        SubCommand::Endpoint { server } => {
            let (mut rd, wt) = socket::create_sockets(&server).await?;
            let mut send_client = client::SendClient::new(wt).start().await;
            rd.listen(db_actor.clone(), send_client.clone()).await;
            send_client.stop(None)?;

        }
        SubCommand::List {detail} => {
            config::handle_list(db_actor.clone(), detail).await;
        }
        SubCommand::Add {editor} => {
            config::handle_add(db_actor.clone(), editor).await;
        }
        SubCommand::Check {name} => {
            config::handle_check(db_actor.clone(), name).await;
        }
        SubCommand::Remove {name} => {
            config::handle_remove(db_actor.clone(), name).await;
        }
    }
    db_actor.stop(None)?;
    Ok(())
}


