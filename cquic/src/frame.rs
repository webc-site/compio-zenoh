//! 线上报文格式：`4 字节 CID 头 + bitcode 编码的帧序列`
//!
//! 一个 UDP 数据报即一个 Packet，帧序列攒批发送以摊薄 IP/UDP 首部开销。
//! `HANDSHAKE_CID` 专用于握手报文，业务报文一律携带连接 CID。

use std::{net::SocketAddr, result};

use bitcode::{Decode, Encode};

use crate::error::{Error, Result};

/// 单个 UDP 数据报上限（1500 - 28 字节 IP/UDP 首部，局域网安全值）
pub const MAX_DATAGRAM_SIZE: usize = 1472;
/// 可靠消息单分片上限（预留 CID 头与 bitcode 编码余量）
pub const MAX_RELIABLE_CHUNK: usize = 1200;
/// 握手报文专用 CID（业务连接禁止使用）
pub const HANDSHAKE_CID: u32 = 0;
/// CID 头字节数
const CID_HEADER_LEN: usize = 4;

/// 协议帧
#[derive(Debug, Clone, Encode, Decode)]
pub enum Frame {
  /// 握手请求（客户端携带随机连接 CID，仅 HANDSHAKE_CID 报文携带）
  Handshake { cid: u32 },
  /// 握手应答（回显连接 CID）
  HandshakeAck { cid: u32 },
  /// 可靠有序分片（fin 标记报文末分片，接收端据以重组）
  Msg { seq: u64, fin: bool, data: Vec<u8> },
  /// 累积确认：seq < next_seq 的分片均已送达
  Ack { next_seq: u64 },
  /// 尽力而为数据报（不确认、不重传，超 MTU 由发送端拒绝）
  Datagram { data: Vec<u8> },
  /// 保活探针（同时刷新对端活跃时间）
  Ping,
  /// 对端请求关闭
  Close,
}

/// 编码完整数据报：CID 头 + bitcode 帧序列
pub fn encode_packet(cid: u32, frames: &[Frame]) -> Vec<u8> {
  let encoded = bitcode::encode(frames);
  let mut pkt = Vec::with_capacity(CID_HEADER_LEN + encoded.len());
  pkt.extend_from_slice(&cid.to_le_bytes());
  pkt.extend_from_slice(&encoded);
  pkt
}

/// 解码数据报，返回 (cid, 帧序列)
pub fn decode_packet(data: &[u8]) -> Result<(u32, Vec<Frame>)> {
  if data.len() < CID_HEADER_LEN {
    return Err(Error::InvalidParam("packet shorter than CID header"));
  }
  let mut cid_bytes = [0u8; CID_HEADER_LEN];
  cid_bytes.copy_from_slice(&data[..CID_HEADER_LEN]);
  let frames = bitcode::decode::<Vec<Frame>>(&data[CID_HEADER_LEN..])?;
  Ok((u32::from_le_bytes(cid_bytes), frames))
}

/// 攒批产物：目标地址 + 完整数据报
pub(crate) type OutPacket = (SocketAddr, Vec<u8>);

/// 数据报攒批器：将多帧合并进单个 MTU 内数据报，摊薄首部与系统调用开销
pub(crate) struct Packer {
  remote: SocketAddr,
  /// 已编码帧序列（不含 CID 头）
  buf: Vec<u8>,
}

impl Packer {
  pub fn new(remote: SocketAddr) -> Self {
    Self {
      remote,
      buf: Vec::with_capacity(MAX_DATAGRAM_SIZE),
    }
  }

  /// 追加一帧；放不下时返回 Err(frame) 由调用方先冲刷
  pub fn push(&mut self, frame: Frame) -> result::Result<(), Frame> {
    let encoded = bitcode::encode(&frame);
    if CID_HEADER_LEN + self.buf.len() + encoded.len() > MAX_DATAGRAM_SIZE {
      return Err(frame);
    }
    self.buf.extend_from_slice(&encoded);
    Ok(())
  }

  /// 追加一帧，放不下则先冲刷（单帧必小于 MTU，冲刷后必能放入）
  pub fn push_or_flush(&mut self, cid: u32, outbox: &mut Vec<OutPacket>, frame: Frame) {
    if let Err(frame) = self.push(frame) {
      self.flush(cid, outbox);
      self.push(frame).ok();
    }
  }

  /// 冲刷为完整数据报；空攒批器不产生输出
  pub fn flush(&mut self, cid: u32, outbox: &mut Vec<OutPacket>) {
    if !self.buf.is_empty() {
      let mut pkt = Vec::with_capacity(CID_HEADER_LEN + self.buf.len());
      pkt.extend_from_slice(&cid.to_le_bytes());
      pkt.append(&mut self.buf);
      outbox.push((self.remote, pkt));
    }
  }
}
