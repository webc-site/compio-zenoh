#![cfg_attr(docsrs, feature(doc_cfg))]

//! compio-zenoh：基于 compio（线程每核模型）与 cquic 的 zenoh 协议兼容传输实现。
//!
//! 线协议与 zenoh v1.10 完全一致（协议版本 0x09）：
//! - 建链：InitSyn/InitAck/OpenSyn/OpenAck 四步握手（含 cookie 校验与版本协商）
//! - 数据：Frame/Fragment 分片重组 + 可靠/尽力双通道 SN 语义 + KeepAlive/Close
//! - 语义：Declare(Subscriber)/Push(Put/Del)，即 pub/sub 最小子集
//!
//! 支持两种链路：
//! - 明文 UDP 单播：与真实 zenoh 的 `udp/{ip}:{port}` 单播链路直接互通（无需 QUIC/TCP）
//! - cquic：自研明文 QUIC 可靠通道（`wedb_quic`），用于 compio-zenoh 节点间互联
//!
//! 消息队列统一使用 crossfire 异步无锁通道。

mod establishment;
mod error;
mod link;
mod session;
mod wire;

pub use error::{Error, Result};
pub use session::{Acceptor, Sample, Session, Subscriber};
