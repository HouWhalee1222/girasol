use prettytable::Table;
use serde::Serialize;
use std::fmt::Display;
use serde_json::Value;
use prettytable::*;
use anyhow::*;
use std::fs::{read_dir, read_link};

pub trait CheckError {
    fn check_error(&self);
}

impl<T> CheckError for anyhow::Result<T> {
    fn check_error(&self) {
        if let Err(e) = self {
            log::error!("{}", e)
        }
    }
}

pub fn to_table<T: Serialize>(s: &T) -> anyhow::Result<Table> {
    serde_json::value::to_value(s)
        .map_err(|x| x.into())
        .and_then(|x| {
            match x {
                Value::Object(e) => {
                    let mut table = Table::new();
                    for (x, y) in e {
                        table.add_row(row![bFy->x, bFb->y.to_table_item()]);
                    }
                    Ok(table)
                }
                _ => Err(
                    anyhow!("to table can only be used to struct")
                )
            }
        })
}

trait ToTableItem {
    fn to_table_item(&self) -> Box<dyn Display>;
}


impl<T: Display> ToTableItem for Option<T> {
    fn to_table_item(&self) -> Box<dyn Display> {
        match self {
            None => Box::new("N/A"),
            Some(e) => Box::new(e.to_string())
        }
    }
}

impl ToTableItem for Value {
    fn to_table_item(&self) -> Box<dyn Display> {
        match self {
            Value::String(x) => Box::new(x.to_string()),
            Value::Null => Box::new("N/A"),
            Value::Bool(value) => Box::new(*value),
            Value::Number(t) => Box::new(t.to_string()),
            Value::Array(t) => {
                if t.is_empty() {
                    Box::new("")
                } else {
                    let mut table = Table::new();
                    for i in t {
                        table.add_row(row![i.to_table_item()]);
                    }
                    Box::new(table)
                }
            }
            Value::Object(t) => {
                let mut table = Table::new();
                for (x, y) in t {
                    table.add_row(row![bFr->x, bfg->y.to_table_item()]);
                }
                Box::new(table)
            }
        }
    }
}


pub fn find_running(s: &str) -> Result<Vec<i32>> {
    read_dir("/proc").map(|entry| {
        entry.filter_map(Result::ok)
            .map(|x|x.file_name())
            .filter_map(|name|name.to_str().and_then(|x|x.parse::<i32>().ok()))
            .filter(|x| read_link(format!("/proc/{}/exe", x))
                .ok()
                .and_then(|x|x.to_str().map(|x| x == s))
                .unwrap_or(false))
            .collect()
    }).map_err(std::io::Error::into)
}

