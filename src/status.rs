use systemstat::{Duration, Platform};
use serde::*;
use log::*;
use std::time::SystemTime;
use xactor::Message;
use typename::TypeName;
#[derive(Serialize, Deserialize, Debug, TypeName)]
pub struct HeartbeatPacket {
    cpu_load: f32,
    cpu_temp: f32,
    mem_load: f32,
    system_uptime: Duration,
    time: SystemTime
}

impl Message for HeartbeatPacket { type Result = (); }

pub fn get_status() -> HeartbeatPacket {
    let platform = systemstat::platform::linux::PlatformImpl::new();
    let cpu_temp = platform.cpu_temp().unwrap();
    let mem_status = platform.memory().unwrap();
    let system_uptime = platform.uptime().unwrap();
    let cpu_load = platform.load_average().unwrap().one;
    let res = HeartbeatPacket {
        cpu_load, cpu_temp, system_uptime,
        mem_load: 1.0 - mem_status.free.as_u64() as f32 / mem_status.total.as_u64() as f32,
        time: std::time::SystemTime::now()
    };
    debug!("status get: {:#?}", res);
    res
}