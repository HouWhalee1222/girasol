use std::io::Write;

use anyhow::*;
use log::*;
use structopt::*;
use xactor::Addr;

use crate::database::{DbMsg, DbReply, TraceModel};
use crate::utils::{CheckError, to_table};

#[derive(StructOpt, Debug)]
pub enum SubCommand {
    #[structopt(about = "Start the endpoint")]
    Endpoint {
        #[structopt(short, long, env = "GIRASOL_SERVER", help="The server websocket address")]
        server: String
    },
    #[structopt(about = "Add new trace model")]
    Add {
        #[structopt(short, long, env = "EDITOR", default_value = "nano", help="The editor to use")]
        editor: String
    },
    #[structopt(about = "Remove a trace model")]
    Remove {
        #[structopt(short, long, help="The name of the model")]
        name: String
    },
    #[structopt(about = "List all trace models")]
    List {
        #[structopt(short, long, help="Whether to show detailed information in json")]
        detail: bool
    },
    #[structopt(about = "Check one trace model")]
    Check {
        #[structopt(short, long, help="The name of the model")]
        name: String
    },
    #[structopt(about = "Local run")]
    Local {
        #[structopt(short, long, help="The name of the model")]
        name: String,
        #[structopt(short, long, help="Round to go")]
        round: usize,
        #[structopt(short, long, help="Output file pattern")]
        pattern: String
    }
}


#[derive(StructOpt, Debug)]
pub struct Config {
    #[structopt(short = "d", long, env = "GIRASOL_HOME", help = "The home directory of Girasol")]
    pub home: String,
    #[structopt(subcommand)]
    pub subcommand: SubCommand,
}

pub async fn handle_list(mut db: Addr<crate::database::DataActor>, detail: bool) {
    match db.call(DbMsg::QueryAll).await
        .map_err(|x| x.into())
        .and_then(|x| x) {
        Err(e) => error!("{}", e),
        Ok(DbReply::AllList(list)) => {
            if detail {
                simd_json::to_string_pretty(&list).map(|x| println!("{}", x))
                    .map_err(|x| x.into())
                    .check_error()
            } else {
                let list: Vec<String> = list.into_iter().map(|x| x.name)
                    .collect();
                simd_json::to_string_pretty(&list).map(|x| println!("{}", x))
                    .map_err(|x| x.into())
                    .check_error()
            }
        }
        _ => unsafe { std::intrinsics::unreachable(); }
    }
}

pub async fn handle_add(mut db: Addr<crate::database::DataActor>, editor: String) {
    let content: Result<TraceModel, Error> = tempfile::NamedTempFile::new()
        .map_err(|x| x.into())
        .and_then(|mut file| {
            simd_json::to_string_pretty(&crate::database::TraceModel::default())
                .map_err(|x| x.into())
                .and_then(|x| file.write_all(x.as_bytes())
                    .map_err(|x| x.into()))
                .map(|_| file)
        })
        .and_then(|file| {
            std::process::Command::new(editor)
                .arg(file.path())
                .spawn()
                .and_then(|mut x| x.wait())
                .map_err(|x| x.into())
                .and_then(|x|
                    if x.success() {
                        file.reopen().map_err(|x| x.into())
                    } else {
                        Err(anyhow!("editor returned unexpected code: {:?}", x.code()))
                    }
                )
        })
        .and_then(|x| {
            simd_json::from_reader(x)
                .map_err(|x| x.into())
        });
    match content {
        Ok(model) => {
            to_table(&model).map(|x| x.printstd())
                .check_error();
            println!("are you sure to add: {} [Y/n]", model.name);
            let mut line = String::new();
            if let Err(e) = std::io::stdin().read_line(&mut line) {
                error!("{}", e);
                async_std::process::exit(1);
            }
            if "y" != line.trim().to_ascii_lowercase() {
                async_std::process::exit(0);
            }
            if model.name.is_empty() {
                error!("cannot add empty name");
                async_std::process::exit(1);
            }
            match db.call(DbMsg::Add(model)).await
                .map_err(|x| x.into())
                .and_then(|x| x) {
                Err(e) => error!("{}", e),
                _ => info!("added successfully")
            }
        }
        Err(e) => error!("{}", e)
    }
}

pub async fn handle_check(mut db: Addr<crate::database::DataActor>, name: String) -> bool {
    match db.call(DbMsg::Get(name)).await
        .map_err(|x| x.into())
        .and_then(|x| x) {
        Ok(DbReply::GetResult(model)) => {
            match to_table(&model) {
                Ok(e) => {
                    e.printstd();
                    return true;
                }
                Err(e) => {
                    error!("{}", e);
                }
            }
        }
        Err(e) => error!("{}", e),
        _ => unsafe { std::intrinsics::unreachable(); }
    }
    false
}

pub async fn handle_remove(mut db: Addr<crate::database::DataActor>, name: String) {
    if !handle_check(db.clone(), name.clone()).await {
        async_std::process::exit(1);
    }
    println!("are you sure to remove: {} [Y/n]", name);
    let mut line = String::new();
    if let Err(e) = std::io::stdin().read_line(&mut line) {
        error!("{}", e);
        async_std::process::exit(1);
    }
    if "y" != line.trim().to_ascii_lowercase() {
        async_std::process::exit(0);
    }
    match db.call(DbMsg::Remove(name)).await
        .map_err(|x| x.into())
        .and_then(|x| x) {
        Err(e) => error!("{}", e),
        _ => info!("removed successfully")
    }
}