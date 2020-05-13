use std::path::Path;

use anyhow::*;
use async_std as astd;
use futures::future::*;
use futures_util::*;
use log::*;
use serde::Serialize;

pub async fn init<A: AsRef<Path>>(home: A) -> Result<sled::Db> {
    sled::open(home.as_ref().join("database"))
        .map_err(|x| x.into())
}

pub async fn query_str<A: AsRef<str>>(key: A, db: &sled::Db) -> Result<String> {
    db.get(key.as_ref())
        .map_err(|x| x.into())
        .and_then(|x|
            x.ok_or(Error::msg(format!("key {} not set", key.as_ref()))))
        .and_then(|x| String::from_utf8(x.to_vec())
            .map_err(|x| x.into()))
}

pub async fn query_json<K, T>(key: K, db: &sled::Db) -> Result<T>
    where for<'de>
          T: serde::Deserialize<'de>,
          K: AsRef<str> {
    db.get(key.as_ref())
        .map_err(|x| x.into())
        .and_then(|x|
            x.ok_or(Error::msg(format!("key {} not set", key.as_ref()))))
        .and_then(|x| {
            let mut v = x.to_vec();
            simd_json::serde::from_slice(v.as_mut_slice())
                .map_err(|x| x.into())
        })
}

pub async fn insert_str<A: AsRef<str>, B: AsRef<str>>(key: A, content: B, db: &sled::Db)
                                                      -> Result<()> {
    match db.contains_key(key.as_ref())
        .map_err(|e| e.into())
        .and_then(|flag| if flag { Err(anyhow!("{} exists", key.as_ref())) } else { Ok(()) })
        .and_then(|_| db.insert(key.as_ref(), content.as_ref()).map_err(|x| x.into())) {
        Ok(_) => db.flush_async().await.map(|e| trace!("{} bytes flushed", e)).map_err(|x| x.into()),
        e => e.map(|_| ())
    }
}

pub async fn insert_obj<A: AsRef<str>, B: Serialize>(key: A, content: B, db: &sled::Db)
                                                      -> Result<()> {
    match db.contains_key(key.as_ref())
        .map_err(|e| e.into())
        .and_then(|flag| if flag { Err(anyhow!("{} exists", key.as_ref())) } else { Ok(()) })
        .and_then(|_| simd_json::to_vec(&content).map_err(|x|x.into()))
        .and_then(|obj| db.insert(key.as_ref(), obj).map_err(|x|x.into())){
        Ok(_) => db.flush_async().await.map(|e| trace!("{} bytes flushed", e)).map_err(|x| x.into()),
        e => e.map(|_| ())
    }
}

