// 版权所有 (c) WeDB 团队与 Microsoft Corporation 保留所有权利。
// 基于 MulanPSL-2.0 协议开源。

use std::{
  collections::HashMap,
  net::SocketAddr,
  sync::Arc,
  time::{Duration, Instant},
};

use bytes::Bytes;
use bytes::Bytes;
use compio::{BufResult, net::UdpSocket, runtime::spawn, time::timeout};
use crossfire::{
  AsyncRx, MAsyncTx, TrySendError,
  mpsc::{Array, bounded_async},
};
use parking_lot::Mutex;

use crate::{
  conn::{
    ConnShared, Connection, DUP_ACK_THRESHOLD, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_MESSAGE_SIZE,
    PING_INTERVAL, RTO_INIT, RTO_MAX, SendCmd, deliver_reliable, enqueue_reliable,
  },
  error::{Error, Result},
  frame::{
    Frame, HANDSHAKE_CID, MAX_DATAGRAM_SIZE, OutPacket, decode_packet, encode_packet,
  },
};

/// accept 队列容量（无 acceptor 时新握手将被丢弃，客户端可重试）
pub const ACCEPT_QUEUE_CAP: usize = 256;
/// 驱动发送任务兜底轮询间隔（重传、保活的最长响应延迟）
const SEND_TICK: Duration = Duration::from_millis(20);
/// 握手重传间隔
const HANDSHAKE_RETRY: Duration = Duration::from_millis(300);

/// 驱动全局状态：连接 CID 注册表
#[derive(Default)]
struct DriverState {
  conns: HashMap<u32, Arc<ConnShared>>,
}

/// 通信端点：单个 UDP 套接字 + 收发双驱动任务
///
/// `bind` 须在 compio 运行时上下文中调用；`accept` 供服务端接入，
/// `connect` 供客户端主动握手（同一端点可同时扮演两种角色）
#[derive(Clone)]
pub struct Endpoint {
  socket: Arc<UdpSocket>,
  local_addr: SocketAddr,
  state: Arc<Mutex<DriverState>>,
  accept_rx: Arc<AsyncRx<Array<(Connection, SocketAddr)>>>,
  notify_tx: MAsyncTx<Array<()>>,
}

impl Endpoint {
  /// 绑定本地地址并启动收发驱动任务
  pub async fn bind(addr: SocketAddr) -> Result<Self> {
    let socket = Arc::new(UdpSocket::bind(addr).await?);
    let local_addr = socket.local_addr()?;
    let (notify_tx, notify_rx) = bounded_async::<()>(64);
    let (accept_tx, accept_rx) = bounded_async::<(Connection, SocketAddr)>(ACCEPT_QUEUE_CAP);
    let state = Arc::new(Mutex::new(DriverState::default()));
    spawn(send_task(socket.clone(), state.clone(), notify_rx));
    spawn(recv_task(
      socket.clone(),
      state.clone(),
      accept_tx,
      notify_tx.clone(),
    ));
    Ok(Self {
      socket,
      local_addr,
      state,
      accept_rx: Arc::new(accept_rx),
      notify_tx,
    })
  }

  /// 本地绑定地址
  #[inline]
  pub fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }

  /// 接收新接入的连接（对端握手完成即返回）
  pub async fn accept(&self) -> Result<(Connection, SocketAddr)> {
    self.accept_rx.recv().await.map_err(|_| Error::Closed)
  }

  /// 主动连接远端：携带随机 CID 发起握手，带重传，超时返回 `HandshakeTimeout`
  pub async fn connect(&self, remote: SocketAddr) -> Result<Connection> {
    let cid = loop {
      let cid = fastrand::u32(1..u32::MAX);
      if !self.state.lock().conns.contains_key(&cid) {
        break cid;
      }
    };
    let shared = Arc::new(ConnShared::new(cid, remote, self.notify_tx.clone()));
    self.state.lock().conns.insert(cid, shared.clone());
    let pkt = encode_packet(HANDSHAKE_CID, &[Frame::Handshake { cid }]);
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let mut result = Err(Error::HandshakeTimeout);
    loop {
      let BufResult(res, b) = self.socket.send_to(pkt.clone(), remote).await;
      drop(b);
      if let Err(e) = res {
        log::warn!("handshake send to {remote} failed: {e}");
      }
      let wait = HANDSHAKE_RETRY.min(deadline.saturating_duration_since(Instant::now()));
      match timeout(wait, shared.established_rx.recv()).await {
        Ok(Ok(())) => {
          result = Ok(Connection::new(shared));
          break;
        }
        Ok(Err(_)) => {
          result = Err(Error::Closed);
          break;
        }
        Err(_) => {} // 单次等待超时，重传握手
      }
      if Instant::now() >= deadline {
        break;
      }
    }
    if result.is_err() {
      self.state.lock().conns.remove(&cid);
    }
    result
  }
}

/// 接收驱动任务：解码数据报并分发至握手处理或对应连接
async fn recv_task(
  socket: Arc<UdpSocket>,
  state: Arc<Mutex<DriverState>>,
  accept_tx: MAsyncTx<Array<(Connection, SocketAddr)>>,
  notify_tx: MAsyncTx<Array<()>>,
) {
  let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];
  loop {
    buf.resize(MAX_DATAGRAM_SIZE, 0);
    let BufResult(res, b) = socket.recv_from(buf).await;
    buf = b;
    match res {
      Ok((len, remote)) => {
        handle_packet(&state, &accept_tx, &socket, &buf[..len], remote, &notify_tx).await;
      }
      Err(e) => log::warn!("udp recv failed: {e}"),
    }
  }
}

/// 解码并处理单个数据报
async fn handle_packet(
  state: &Mutex<DriverState>,
  accept_tx: &MAsyncTx<Array<(Connection, SocketAddr)>>,
  socket: &UdpSocket,
  data: &[u8],
  remote: SocketAddr,
  notify_tx: &MAsyncTx<Array<()>>,
) {
  let (cid, frames) = match decode_packet(data) {
    Ok(v) => v,
    Err(e) => {
      log::debug!("malformed datagram from {remote}: {e}");
      return;
    }
  };
  if cid == HANDSHAKE_CID {
    handle_handshake(frames, remote, state, accept_tx, socket, notify_tx).await;
    return;
  }
  let Some(shared) = state.lock().conns.get(&cid).cloned() else {
    log::debug!("unknown cid {cid:#010x} from {remote}");
    return;
  };
  if shared.is_closed() {
    return;
  }
  let mut ci = shared.inner.lock();
  ci.last_recv = Instant::now();
  let mut saw_msg = false;
  for frame in frames {
    match frame {
      Frame::Msg { seq, fin, data } => {
        saw_msg = true;
        if seq < ci.recv_next {
          continue; // 重复分片，稍后统一累积确认
        }
        if seq > ci.recv_next {
          log::debug!(
            "conn {:#010x} gap at seq {seq}, waiting retransmit",
            shared.cid
          );
          continue; // 乱序丢弃，等待发送端回退重传
        }
        ci.recv_next += 1;
        if ci.assembling.len() + data.len() > MAX_MESSAGE_SIZE {
          log::error!("conn {:#010x} message too large, closing", shared.cid);
          shared.mark_closed();
          return;
        }
        ci.assembling.extend_from_slice(&data);
        if fin && !deliver_reliable(&shared, &mut ci) {
          log::error!("conn {:#010x} reliable queue overflow, closing", shared.cid);
          shared.mark_closed();
          return;
        }
      }
      Frame::Ack { next_seq } => {
        while let Some(front) = ci.inflight.front() {
          if front.seq >= next_seq {
            break;
          }
          let f = ci.inflight.pop_front().unwrap();
          ci.inflight_bytes -= f.data.len();
          if f.fin {
            ci.rto = RTO_INIT; // 确认有进展，恢复初始 RTO
          }
        }
        if next_seq > ci.last_ack {
          ci.last_ack = next_seq;
          ci.dup_acks = 0;
        } else if next_seq < ci.send_next && !ci.inflight.is_empty() {
          ci.dup_acks += 1;
          if ci.dup_acks >= DUP_ACK_THRESHOLD {
            ci.fast_retransmit = true;
            ci.dup_acks = 0;
          }
        }
      }
      Frame::Datagram { data } => {
        let _ = shared.datagram_tx.try_send(data.into());
      }
      Frame::Close => shared.mark_closed(),
      Frame::Ping => {}
      Frame::Handshake { .. } | Frame::HandshakeAck { .. } => {}
    }
    if shared.is_closed() {
      break;
    }
  }
  if saw_msg && !shared.is_closed() {
    ci.packer.push_or_flush(
      shared.cid,
      &mut ci.outbox,
      Frame::Ack {
        next_seq: ci.recv_next,
      },
    );
  }
}

/// 处理握手报文：服务端注册新连接并应答，客户端处理握手完成
async fn handle_handshake(
  frames: Vec<Frame>,
  remote: SocketAddr,
  state: &Mutex<DriverState>,
  accept_tx: &MAsyncTx<Array<(Connection, SocketAddr)>>,
  socket: &UdpSocket,
  notify_tx: &MAsyncTx<Array<()>>,
) {
  for frame in frames {
    match frame {
      Frame::Handshake { cid } => {
        if cid == HANDSHAKE_CID {
          continue;
        }
        let registered = state.lock().conns.contains_key(&cid);
        if !registered {
          let shared = Arc::new(ConnShared::new(cid, remote, notify_tx.clone()));
          shared.inner.lock().established = true; // 服务端接入即视为握手完成
          match accept_tx.try_send((Connection::new(shared.clone()), remote)) {
            Ok(()) | Err(TrySendError::Full(_)) => {
              state.lock().conns.insert(cid, shared);
            }
            Err(TrySendError::Disconnected(_)) => {
              log::debug!("handshake from {remote} dropped: no acceptor");
              continue;
            }
          }
        }
        let pkt = encode_packet(HANDSHAKE_CID, &[Frame::HandshakeAck { cid }]);
        let BufResult(res, b) = socket.send_to(pkt, remote).await;
        drop(b);
        if let Err(e) = res {
          log::warn!("send handshake ack to {remote} failed: {e}");
        }
      }
      Frame::HandshakeAck { cid } => {
        if let Some(shared) = state.lock().conns.get(&cid).cloned() {
          let mut ci = shared.inner.lock();
          if !ci.established {
            ci.established = true;
            let _ = shared.established_tx.try_send(());
          }
        }
      }
      _ => {}
    }
  }
}

/// 发送驱动任务：排空用户发送命令、执行重传与保活，并统一发出攒批数据报
async fn send_task(
  socket: Arc<UdpSocket>,
  state: Arc<Mutex<DriverState>>,
  mut notify_rx: AsyncRx<Array<()>>,
) {
  let mut outbox: Vec<OutPacket> = Vec::new();
  loop {
    // 通知或兜底 tick 任一到达即触发一轮处理
    let _ = timeout(SEND_TICK, notify_rx.recv()).await;
    while notify_rx.try_recv().is_ok() {}

    outbox.clear();
    let conns: Vec<Arc<ConnShared>> = state.lock().conns.values().cloned().collect();
    let mut dead = Vec::new();
    for shared in conns {
      if shared.is_closed() {
        dead.push(shared.cid);
        continue;
      }
      let mut ci = shared.inner.lock();
      // 用户发送命令：握手完成后方可入窗（保证对端可路由）
      if ci.established {
        while let Ok(cmd) = shared.send_rx.try_recv() {
          match cmd {
            SendCmd::Reliable(data) => {
              if let Some(rest) = enqueue_reliable(&mut ci, data) {
                ci.pending.push_back(SendCmd::Reliable(rest));
                break; // 发送窗口已满
              }
            }
            SendCmd::Datagram(data) => ci.packer.push_or_flush(
              shared.cid,
              &mut ci.outbox,
              Frame::Datagram {
                data: data.to_vec(),
              },
            ),
            SendCmd::Close => {
              ci.packer
                .push_or_flush(shared.cid, &mut ci.outbox, Frame::Close);
              shared.mark_closed();
              break;
            }
          }
        }
        // 窗口腾出后续发积压命令
        while let Some(SendCmd::Reliable(data)) = ci.pending.front() {
          match enqueue_reliable(&mut ci, data.clone()) {
            Some(rest) => {
              ci.pending.pop_front();
              ci.pending.push_front(SendCmd::Reliable(rest));
              break;
            }
            None => ci.pending.pop_front(),
          }
        }
        // 重传：重复 ACK 快速重传或最老分片超时（go-back-N 全量回退）
        if let Some(oldest) = ci.inflight.front() {
          let due = ci.fast_retransmit || oldest.sent_at.elapsed() >= ci.rto;
          if due {
            for f in ci.inflight.iter_mut() {
              ci.packer.push_or_flush(
                shared.cid,
                &mut ci.outbox,
                Frame::Msg {
                  seq: f.seq,
                  fin: f.fin,
                  data: f.data.to_vec(),
                },
              );
              f.sent_at = Instant::now();
            }
            ci.rto = (ci.rto + ci.rto).min(RTO_MAX);
            ci.fast_retransmit = false;
          }
        }
        // 空闲判定与保活探针
        if ci.last_recv.elapsed() >= IDLE_TIMEOUT {
          log::info!("conn {:#010x} idle timeout", shared.cid);
          shared.mark_closed();
          dead.push(shared.cid);
          continue;
        }
        if ci.last_sent.elapsed() >= PING_INTERVAL {
          ci.packer
            .push_or_flush(shared.cid, &mut ci.outbox, Frame::Ping);
          ci.last_sent = Instant::now();
        }
        ci.packer.flush(shared.cid, &mut ci.outbox);
        outbox.append(&mut ci.outbox);
      }
    }
    if !dead.is_empty() {
      let mut st = state.lock();
      for cid in dead {
        st.conns.remove(&cid);
      }
    }
    for (remote, pkt) in outbox.drain(..) {
      let BufResult(res, b) = socket.send_to(pkt, remote).await;
      drop(b);
      if let Err(e) = res {
        log::warn!("send to {remote} failed: {e}");
      }
    }
  }
}
