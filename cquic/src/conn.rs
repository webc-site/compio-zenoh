use std::{
  collections::VecDeque,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::{Duration, Instant},
};

use bytes::Bytes;
use crossfire::{
  AsyncRx, MAsyncTx,
  mpsc::{Array, bounded_async},
};
use parking_lot::Mutex;

use crate::{
  error::{Error, Result},
  frame::{Frame, MAX_RELIABLE_CHUNK, OutPacket, Packer},
};

/// 在途字节窗口上限
pub const MAX_INFLIGHT_BYTES: usize = 256 * 1024;
/// 在途分片数窗口上限
pub const MAX_INFLIGHT_MSGS: usize = 1024;
/// 初始重传超时
pub const RTO_INIT: Duration = Duration::from_millis(200);
/// 重传超时上限（指数退避封顶）
pub const RTO_MAX: Duration = Duration::from_secs(2);
/// 重复 ACK 快速重传阈值
pub const DUP_ACK_THRESHOLD: u32 = 3;
/// 保活探针间隔
pub const PING_INTERVAL: Duration = Duration::from_secs(5);
/// 空闲判定超时（超过则判死连接）
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// 握手超时
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);

/// 可靠接收队列容量（驱动向用户投递方向）
pub(crate) const RELIABLE_QUEUE_CAP: usize = 4096;
/// 尽力而为接收队列容量
pub(crate) const DATAGRAM_QUEUE_CAP: usize = 1024;
/// 用户发送命令队列容量
pub(crate) const SEND_QUEUE_CAP: usize = 4096;
/// 重组缓冲上限：超限视为异常流，强制断连
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// 用户到驱动的发送命令
#[derive(Debug)]
pub(crate) enum SendCmd {
  /// 可靠有序消息（驱动负责分片、确认与重传）
  Reliable(Bytes),
  /// 尽力而为数据报
  Datagram(Bytes),
  /// 请求关闭连接
  Close,
}

/// 连接协议状态（驱动收发任务共享，互斥锁保护）
pub(crate) struct ConnInner {
  /// 握手是否完成
  pub established: bool,
  /// 下一个待发送分片序号
  pub send_next: u64,
  /// 已发送未确认的在途分片（按 seq 升序）
  pub inflight: VecDeque<Inflight>,
  /// 在途分片总字节数
  pub inflight_bytes: usize,
  /// 发送窗口耗尽时溢出的待发命令
  pub pending: VecDeque<SendCmd>,
  /// 下一个期望接收的分片序号
  pub recv_next: u64,
  /// 跨分片重组缓冲
  pub assembling: Vec<u8>,
  /// 重复 ACK 快速重传标记
  pub fast_retransmit: bool,
  /// 上次累积确认位置（重复 ACK 检测）
  pub last_ack: u64,
  /// 连续重复 ACK 计数
  pub dup_acks: u32,
  /// 重传超时（确认有进展时重置，连续超时指数退避）
  pub rto: Duration,
  /// 最近一次收到对端报文时间
  pub last_recv: Instant,
  /// 最近一次向对端发送报文时间
  pub last_sent: Instant,
  /// 连接 CID（攒批器填充报文头用）
  pub cid: u32,
  /// 报文攒批器
  pub packer: Packer,
  /// 攒批产物出站缓冲（发送任务统一冲刷）
  pub outbox: Vec<OutPacket>,
}

/// 在途分片
pub(crate) struct Inflight {
  pub seq: u64,
  /// 是否为报文末分片
  pub fin: bool,
  pub data: Bytes,
  pub sent_at: Instant,
}

/// 连接共享体（用户句柄与驱动收发任务共享）
pub(crate) struct ConnShared {
  /// 连接 CID（线上报文路由凭据）
  pub cid: u32,
  pub remote: SocketAddr,
  pub closed: AtomicBool,
  /// 协议状态
  pub inner: Mutex<ConnInner>,
  /// 用户 -> 驱动发送命令
  pub send_tx: MAsyncTx<Array<SendCmd>>,
  pub send_rx: AsyncRx<Array<SendCmd>>,
  /// 驱动 -> 用户可靠消息
  pub reliable_tx: MAsyncTx<Array<Bytes>>,
  pub reliable_rx: AsyncRx<Array<Bytes>>,
  /// 驱动 -> 用户尽力而为数据报
  pub datagram_tx: MAsyncTx<Array<Bytes>>,
  pub datagram_rx: AsyncRx<Array<Bytes>>,
  /// 客户端握手完成通知
  pub established_tx: MAsyncTx<Array<()>>,
  pub established_rx: AsyncRx<Array<()>>,
  /// 唤醒驱动发送任务
  pub notify_tx: MAsyncTx<Array<()>>,
}

impl ConnShared {
  pub fn new(cid: u32, remote: SocketAddr, notify_tx: MAsyncTx<Array<()>>) -> Self {
    let (send_tx, send_rx) = bounded_async(SEND_QUEUE_CAP);
    let (reliable_tx, reliable_rx) = bounded_async(RELIABLE_QUEUE_CAP);
    let (datagram_tx, datagram_rx) = bounded_async(DATAGRAM_QUEUE_CAP);
    let (established_tx, established_rx) = bounded_async(1);
    Self {
      cid,
      remote,
      closed: AtomicBool::new(false),
      inner: Mutex::new(ConnInner {
        established: false,
        send_next: 0,
        inflight: VecDeque::new(),
        inflight_bytes: 0,
        pending: VecDeque::new(),
        recv_next: 0,
        assembling: Vec::new(),
        fast_retransmit: false,
        last_ack: 0,
        dup_acks: 0,
        rto: RTO_INIT,
        last_recv: Instant::now(),
        last_sent: Instant::now(),
        cid,
        packer: Packer::new(remote),
        outbox: Vec::new(),
      }),
      send_tx,
      send_rx,
      reliable_tx,
      reliable_rx,
      datagram_tx,
      datagram_rx,
      established_tx,
      established_rx,
      notify_tx,
    }
  }

  #[inline]
  pub fn is_closed(&self) -> bool {
    self.closed.load(Ordering::Acquire)
  }

  /// 标记关闭：驱动随即释放该连接，用户接收端排空后返回 `Error::Closed`
  #[inline]
  pub fn mark_closed(&self) {
    self.closed.store(true, Ordering::Release);
  }
}

/// 连接句柄（可克隆；克隆共享发送端，接收为单消费者语义）
#[derive(Clone)]
pub struct Connection {
  shared: Arc<ConnShared>,
}

impl std::fmt::Debug for Connection {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Connection")
      .field("cid", &self.shared.cid)
      .field("remote", &self.shared.remote)
      .finish()
  }
}

impl Connection {
  pub(crate) fn new(shared: Arc<ConnShared>) -> Self {
    Self { shared }
  }

  /// 连接 CID
  #[inline]
  pub fn id(&self) -> u32 {
    self.shared.cid
  }

  /// 对端地址
  #[inline]
  pub fn remote_addr(&self) -> SocketAddr {
    self.shared.remote
  }

  /// 连接是否已关闭
  #[inline]
  pub fn is_closed(&self) -> bool {
    self.shared.is_closed()
  }

  /// 发送可靠有序消息（自动分片，接收端重组，窗口满时背压等待）
  pub async fn send_reliable(&self, data: impl Into<Bytes>) -> Result<()> {
    self
      .shared
      .send_tx
      .send(SendCmd::Reliable(data.into()))
      .await
      .map_err(|_| Error::Closed)?;
    let _ = self.shared.notify_tx.try_send(());
    Ok(())
  }

  /// 发送尽力而为数据报（不确认不重传；超分片上限直接拒绝）
  pub async fn send_datagram(&self, data: impl Into<Bytes>) -> Result<()> {
    let data = data.into();
    if data.len() > MAX_RELIABLE_CHUNK {
      return Err(Error::InvalidParam("datagram exceeds MTU budget"));
    }
    self
      .shared
      .send_tx
      .send(SendCmd::Datagram(data))
      .await
      .map_err(|_| Error::Closed)?;
    let _ = self.shared.notify_tx.try_send(());
    Ok(())
  }

  /// 接收可靠有序消息；连接关闭并排空后返回 `Error::Closed`
  pub async fn recv_reliable(&self) -> Result<Bytes> {
    self
      .shared
      .reliable_rx
      .recv()
      .await
      .map_err(|_| Error::Closed)
  }

  /// 接收尽力而为数据报；连接关闭并排空后返回 `Error::Closed`
  pub async fn recv_datagram(&self) -> Result<Bytes> {
    self
      .shared
      .datagram_rx
      .recv()
      .await
      .map_err(|_| Error::Closed)
  }

  /// 请求关闭连接（驱动将向对端发送 Close 帧）
  pub fn close(&self) {
    if !self.shared.is_closed() {
      self.shared.mark_closed();
      let _ = self.shared.send_tx.try_send(SendCmd::Close);
      let _ = self.shared.notify_tx.try_send(());
    }
  }
}

/// 将可靠消息分片入队在途窗口并攒批编码；窗口不足时返回未入队的剩余字节
///
/// 空消息合法：编码为单个空 fin 分片，用于纯边界信号
pub(crate) fn enqueue_reliable(inner: &mut ConnInner, data: Bytes) -> Option<Bytes> {
  let mut off = 0usize;
  loop {
    let end = (off + MAX_RELIABLE_CHUNK).min(data.len());
    let window_left = MAX_INFLIGHT_BYTES - inner.inflight_bytes;
    if inner.inflight.len() >= MAX_INFLIGHT_MSGS || end - off > window_left {
      return (off < data.len()).then(|| data.slice(off..));
    }
    let chunk = data.slice(off..end);
    inner.inflight_bytes += chunk.len();
    inner.inflight.push_back(Inflight {
      seq: inner.send_next,
      fin: end == data.len(),
      data: chunk.clone(),
      sent_at: Instant::now(),
    });
    inner.packer.push_or_flush(
      inner.cid,
      &mut inner.outbox,
      Frame::Msg {
        seq: inner.send_next,
        fin: end == data.len(),
        data: chunk.to_vec(),
      },
    );
    inner.send_next += 1;
    inner.last_sent = Instant::now();
    if end == data.len() {
      return None;
    }
    off = end;
  }
}

/// 重组完成投递用户可靠队列；溢出返回 false（调用方断连）
#[inline]
pub(crate) fn deliver_reliable(conn: &ConnShared, inner: &mut ConnInner) -> bool {
  let msg = std::mem::take(&mut inner.assembling).into();
  conn.reliable_tx.try_send(msg).is_ok()
}
