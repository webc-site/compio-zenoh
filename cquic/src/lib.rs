#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(clippy::absolute_paths)]

//! 自研精简明文 QUIC：基于 compio UDP 的零加密高可用集群通信总线传输层。
//!
//! 参考 zenoh 明文 QUIC（`udp/{addr}?rel=reliable&mixed_rel=true`）的通信模型，
//! 自研更精简的线协议，不含任何 TLS/rustls/quinn 依赖，零加密开销：
//!
//! - 可靠通道：CID 握手 + 单调 seq + 累积 ACK + 重复 ACK 快速重传 + RTO go-back-N
//! - 尽力通道：单帧数据报，不确认不重传，专供高频心跳
//! - 攒批发送：多个帧合并进单个数据报，摊薄 IP/UDP 首部与系统调用开销
//! - 序列化：bitcode 极致紧凑二进制编码

mod conn;
mod endpoint;
mod error;
mod frame;

pub use conn::{
  Connection, DUP_ACK_THRESHOLD, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_INFLIGHT_BYTES,
  MAX_INFLIGHT_MSGS, MAX_MESSAGE_SIZE, PING_INTERVAL, RTO_INIT, RTO_MAX,
};
pub use endpoint::{ACCEPT_QUEUE_CAP, Endpoint};
pub use error::{Error, Result};
pub use frame::{
  Frame, HANDSHAKE_CID, MAX_DATAGRAM_SIZE, MAX_RELIABLE_CHUNK, decode_packet, encode_packet,
};
