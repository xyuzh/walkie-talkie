//! Length-prefixed CBOR frames over a byte stream (iroh `SendStream` / `RecvStream`).
//!
//! Wire format: `u32_be(len) || cbor_bytes`. `len <= MAX_FRAME_BYTES`.

use anyhow::{bail, Context, Result};
use iroh::endpoint::{ReadExactError, RecvStream, SendStream};

pub const MAX_FRAME_BYTES: u32 = 16 * 1024 * 1024; // 16 MiB

/// Typed errors from `read_cbor_frame`, so callers can distinguish a normal end-of-stream
/// (peer finished its send side) from a real failure without matching on error strings.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// The peer cleanly finished the stream at a frame boundary (zero bytes into the next
    /// length prefix). This is the expected way a stream ends.
    #[error("stream finished cleanly")]
    CleanEof,
    /// The advertised frame length exceeds `MAX_FRAME_BYTES`.
    #[error("incoming frame too large: {0} bytes")]
    TooLarge(u32),
    /// A transport read error, or a truncated frame (stream ended mid-frame).
    #[error("stream read error: {0}")]
    Read(#[from] ReadExactError),
}

pub async fn write_cbor_frame(send: &mut SendStream, body: &[u8]) -> Result<()> {
    if body.len() as u64 > MAX_FRAME_BYTES as u64 {
        bail!("frame too large: {} bytes", body.len());
    }
    let len = (body.len() as u32).to_be_bytes();
    send.write_all(&len).await.context("write frame len")?;
    send.write_all(body).await.context("write frame body")?;
    Ok(())
}

/// Read one length-prefixed CBOR frame. Returns `Err(FrameError::CleanEof)` when the peer has
/// finished the stream at a frame boundary — callers treat that as a normal loop exit.
pub async fn read_cbor_frame(recv: &mut RecvStream) -> std::result::Result<Vec<u8>, FrameError> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        // Zero bytes into the length prefix = peer finished cleanly.
        Err(ReadExactError::FinishedEarly(0)) => return Err(FrameError::CleanEof),
        // Any other read error (incl. a partial length prefix = truncated) is a real failure.
        Err(e) => return Err(FrameError::Read(e)),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge(len));
    }
    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf).await.map_err(FrameError::Read)?;
    Ok(buf)
}
