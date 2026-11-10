//! One TCP connection carrying frames, with a time limit on every network call (spec 10, 14.10).
//!
//! A reader thread decodes whatever arrives and hands complete frames over a channel; the owner sends from its own
//! thread. Blocking I/O and threads only, no async runtime.

use crate::frames::{self, Decoder, Frame, FrameError};
use crate::meta;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Why a session ended.
#[derive(Debug)]
pub enum End {
    /// The connection is gone (closed, silent for too long, a send that did not drain, a file error).
    Lost(String),
    /// The peer sent a bad frame: answer ERROR with this code and close (spec 14.2).
    Bad(FrameError),
}

impl From<io::Error> for End {
    fn from(e: io::Error) -> Self {
        End::Lost(e.to_string())
    }
}

impl From<FrameError> for End {
    fn from(e: FrameError) -> Self {
        End::Bad(e)
    }
}

enum Incoming {
    Frames(Vec<Frame>),
    Bad(FrameError),
    Closed(String),
}

pub struct Conn {
    stream: TcpStream,
    rx: Receiver<Incoming>,
    pending: std::collections::VecDeque<Frame>,
    last_received: Arc<Mutex<Instant>>,
    reader: Option<JoinHandle<()>>,
    pub last_sent: Instant,
    /// Protocol version written into byte 2 of every frame sent (spec 14.5).
    pub version: u8,
    pub send_timeout: Duration,
}

impl Conn {
    /// Connects to `addr` ("host:port") within `timeout`.
    pub fn connect(addr: &str, timeout: Duration) -> io::Result<Conn> {
        let mut last = io::Error::new(io::ErrorKind::NotFound, format!("no address for {}", addr));
        for a in addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&a, timeout) {
                Ok(s) => return Conn::from_stream(s, timeout),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    /// Wraps an accepted connection (the hub's side).
    pub fn from_stream(stream: TcpStream, timeout: Duration) -> io::Result<Conn> {
        stream.set_nodelay(true)?;
        let mut read_side = stream.try_clone()?;
        let (tx, rx) = mpsc::channel();
        let last_received = Arc::new(Mutex::new(Instant::now()));
        let heard = last_received.clone();
        let reader = thread::spawn(move || {
            let mut dec = Decoder::new();
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = match read_side.read(&mut buf) {
                    Ok(0) => {
                        let _ = tx.send(Incoming::Closed("connection closed by peer".into()));
                        return;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        let _ = tx.send(Incoming::Closed(e.to_string()));
                        return;
                    }
                };
                *heard.lock().unwrap() = Instant::now();
                match dec.feed(&buf[..n]) {
                    Ok(fs) if fs.is_empty() => {}
                    Ok(fs) => {
                        if tx.send(Incoming::Frames(fs)).is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Incoming::Bad(e));
                        return;
                    }
                }
            }
        });
        Ok(Conn {
            stream,
            rx,
            pending: Default::default(),
            last_received,
            reader: Some(reader),
            last_sent: Instant::now(),
            version: frames::VERSION,
            send_timeout: timeout,
        })
    }

    pub fn send(&mut self, frame: &Frame) -> Result<(), End> {
        let mut f = frame.clone();
        f.version = self.version;
        let raw = frames::encode(&f)?;
        self.stream.set_write_timeout(Some(self.send_timeout))?;
        self.stream.write_all(&raw).map_err(|e| match e.kind() {
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => End::Lost(format!(
                "send timed out after {:.1} s",
                self.send_timeout.as_secs_f64()
            )),
            _ => End::Lost(e.to_string()),
        })?;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// Time since anything at all arrived (heartbeats included).
    pub fn silent_for(&self) -> Duration {
        self.last_received.lock().unwrap().elapsed()
    }

    fn take(&mut self, got: Incoming) -> Result<(), End> {
        match got {
            Incoming::Frames(fs) => {
                self.pending.extend(fs);
                Ok(())
            }
            Incoming::Bad(e) => Err(End::Bad(e)),
            Incoming::Closed(why) => Err(End::Lost(why)),
        }
    }

    fn pop_valid(&mut self) -> Option<Result<Frame, End>> {
        let f = self.pending.pop_front()?;
        Some(frames::validate(&f).map(|_| f).map_err(End::Bad))
    }

    /// The next validated frame if one has arrived, without waiting.
    pub fn try_recv(&mut self) -> Result<Option<Frame>, End> {
        if self.pending.is_empty() {
            match self.rx.try_recv() {
                Ok(got) => self.take(got)?,
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Disconnected) => {
                    return Err(End::Lost("connection closed".into()))
                }
            }
        }
        self.pop_valid().transpose()
    }

    /// The next validated frame, waiting at most `wait`. `Ok(None)` when nothing arrived in that time.
    pub fn recv_wait(&mut self, wait: Duration) -> Result<Option<Frame>, End> {
        if self.pending.is_empty() {
            match self.rx.recv_timeout(wait) {
                Ok(got) => self.take(got)?,
                Err(RecvTimeoutError::Timeout) => return Ok(None),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(End::Lost("connection closed".into()))
                }
            }
        }
        self.pop_valid().transpose()
    }

    /// The next validated frame; `End::Lost` if nothing at all arrives within `timeout`.
    pub fn recv(&mut self, timeout: Duration) -> Result<Frame, End> {
        let end = Instant::now() + timeout;
        loop {
            let left = end.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(End::Lost(format!(
                    "no sign of life for {:.1} s",
                    timeout.as_secs_f64()
                )));
            }
            if let Some(f) = self.recv_wait(left)? {
                return Ok(f);
            }
        }
    }

    /// Spec 14.2: answer a bad frame with ERROR (best effort, at most 1 s), then close.
    pub fn error_and_close(&mut self, e: &FrameError) {
        self.send_timeout = self.send_timeout.min(Duration::from_secs(1));
        let _ = self.send(&Frame::new(
            frames::kind::ERROR,
            meta! {"code" => e.code, "text" => e.text},
        ));
        self.close();
    }

    pub fn close(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Both);
        if let Some(r) = self.reader.take() {
            let _ = r.join();
        }
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.close();
    }
}
