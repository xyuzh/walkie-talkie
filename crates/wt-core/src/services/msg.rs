//! `Msg` stream service: per-stream read/write loops.
//!
//! v0.1 design: each Msg stream is *one-way* — the initiator writes, the receiver reads.
//! Bidirectional messaging is achieved by having both peers open their own stream (which is
//! what reciprocal tokens make natural). v0.2 may add acks on the reverse direction for
//! at-least-once.

use anyhow::{Context, Result};
use iroh::endpoint::{RecvStream, SendStream};
use tokio::sync::mpsc;
use tracing::{debug, warn};
use wt_proto::wire::{MessageFrame, StreamOpen};

use crate::framing::{self, FrameError};

/// Read the very first frame on a freshly-accepted bidi stream and decode it as `StreamOpen`.
pub async fn read_stream_open(recv: &mut RecvStream) -> Result<StreamOpen> {
    let bytes = framing::read_cbor_frame(recv)
        .await
        .context("read StreamOpen")?;
    let so: StreamOpen = ciborium::from_reader(&bytes[..]).context("decode StreamOpen")?;
    Ok(so)
}

/// Write the `StreamOpen` as the first frame of a freshly-opened bidi stream.
pub async fn write_stream_open(send: &mut SendStream, so: &StreamOpen) -> Result<()> {
    let mut buf = Vec::with_capacity(256);
    ciborium::into_writer(so, &mut buf).context("encode StreamOpen")?;
    framing::write_cbor_frame(send, &buf).await
}

/// Receive-loop on an incoming Msg stream: read `MessageFrame`s and forward via `out_tx`.
pub async fn run_recv_loop(mut recv: RecvStream, out_tx: mpsc::Sender<MessageFrame>) -> Result<()> {
    loop {
        let frame_bytes = match framing::read_cbor_frame(&mut recv).await {
            Ok(b) => b,
            Err(FrameError::CleanEof) => {
                debug!("recv loop: clean EOF");
                return Ok(());
            }
            Err(e) => return Err(e).context("read message frame"),
        };
        let mf: MessageFrame =
            ciborium::from_reader(&frame_bytes[..]).context("decode MessageFrame")?;
        if out_tx.send(mf).await.is_err() {
            warn!("recv loop: consumer dropped, closing");
            return Ok(());
        }
    }
}

/// Send-loop on an outgoing Msg stream: pull `MessageFrame`s from `in_rx` and write them.
pub async fn run_send_loop(
    mut send: SendStream,
    mut in_rx: mpsc::Receiver<MessageFrame>,
) -> Result<()> {
    while let Some(mf) = in_rx.recv().await {
        let mut buf = Vec::with_capacity(64 + mf.payload.len());
        ciborium::into_writer(&mf, &mut buf)?;
        framing::write_cbor_frame(&mut send, &buf).await?;
    }
    let _ = send.finish();
    Ok(())
}
