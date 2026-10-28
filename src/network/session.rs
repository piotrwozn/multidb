//! Session lifecycle bounds around pgwire's TLS, authentication and protocol dispatch.

use std::{io, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::StreamExt;
use pgwire::{
    api::{ClientInfo, ErrorHandler, PgWireConnectionState, PgWireServerHandlers},
    error::{PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, PgWireFrontendMessage},
    tokio::server::{PgWireMessageServerCodec, negotiate_tls, process_error, process_message},
};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

use super::{DbPgFactory, NetworkConfig, TlsAcceptor};

#[derive(Clone, Copy)]
pub(super) struct SessionLimits {
    handshake: Duration,
    idle: Duration,
    request: Duration,
    frame_bytes: usize,
}

impl SessionLimits {
    pub(super) fn from_config(config: &NetworkConfig) -> Self {
        Self {
            handshake: Duration::from_millis(config.handshake_timeout_ms.max(1)),
            idle: Duration::from_millis(config.idle_timeout_ms.max(1)),
            request: Duration::from_millis(config.connection_timeout_ms.max(1)),
            frame_bytes: config.max_frame_bytes.max(4),
        }
    }
}

/// Rejects oversized declared lengths before the upstream decoder can reserve body space.
/// The upstream type-specific packet limits still apply, even if the configured cap is larger.
struct BoundedCodec {
    inner: PgWireMessageServerCodec<String>,
    startup: bool,
    frame_bytes: usize,
}

impl Decoder for BoundedCodec {
    type Item = PgWireFrontendMessage;
    type Error = PgWireError;

    fn decode(&mut self, bytes: &mut BytesMut) -> PgWireResult<Option<Self::Item>> {
        let offset = usize::from(!self.startup);
        if bytes.len() < offset + 4 {
            return Ok(None);
        }
        let mut length = [0_u8; 4];
        length.copy_from_slice(&bytes[offset..offset + 4]);
        let length = i32::from_be_bytes(length);
        if length < 4 {
            return Err(
                io::Error::new(io::ErrorKind::InvalidData, "invalid pgwire frame length").into(),
            );
        }
        let length = usize::try_from(length).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid pgwire frame length")
        })?;
        if length > self.frame_bytes {
            return Err(PgWireError::MessageTooLarge(length, self.frame_bytes));
        }
        self.inner.decode(bytes)
    }
}

impl Encoder<PgWireBackendMessage> for BoundedCodec {
    type Error = io::Error;

    fn encode(&mut self, message: PgWireBackendMessage, bytes: &mut BytesMut) -> io::Result<()> {
        self.inner.encode(message, bytes)
    }
}

fn timed_out(phase: &str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, format!("pgwire {phase} timed out"))
}

fn deadline(duration: Duration) -> io::Result<tokio::time::Instant> {
    tokio::time::Instant::now()
        .checked_add(duration)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "pgwire timeout is too large"))
}

pub(super) async fn process_socket(
    socket: TcpStream,
    tls: Option<TlsAcceptor>,
    factory: Arc<DbPgFactory>,
    limits: SessionLimits,
) -> io::Result<()> {
    let handshake_deadline = deadline(limits.handshake)?;
    let socket = tokio::time::timeout_at(handshake_deadline, negotiate_tls::<String>(socket, tls))
        .await
        .map_err(|_| timed_out("handshake"))??;
    let Some(mut socket) = socket else {
        return Ok(());
    };
    let startup = factory.startup_handler();
    let query = factory.simple_query_handler();
    // The concrete handler fixes Statement = String for the socket codec. The
    // factory trait's opaque return type intentionally hides that associated type.
    let extended = Arc::clone(&factory.handler);
    let copy = factory.copy_handler();
    let cancel = factory.cancel_handler();
    let errors = factory.error_handler();
    loop {
        let state = socket.state();
        let authenticating = matches!(
            state,
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        );
        let mut bounded = socket.map_codec(|inner| BoundedCodec {
            inner,
            startup: matches!(state, PgWireConnectionState::AwaitingStartup),
            frame_bytes: limits.frame_bytes,
        });
        let message = if authenticating {
            tokio::time::timeout_at(handshake_deadline, bounded.next())
                .await
                .map_err(|_| timed_out("handshake"))?
        } else {
            // This deadline exists only while receiving the next frame. An executing query
            // or a busy response is governed by request/database budgets, never idle time.
            tokio::time::timeout(limits.idle, bounded.next())
                .await
                .map_err(|_| timed_out("idle read"))?
        };
        socket = bounded.map_codec(|codec| codec.inner);
        let Some(message) = message else {
            return Ok(());
        };
        let message = message.map_err(io::Error::from)?;
        let is_extended = match state {
            PgWireConnectionState::CopyInProgress(is_extended) => is_extended,
            _ => message.is_extended_query(),
        };
        let request_deadline = deadline(limits.request)?;
        let request_deadline = if authenticating {
            request_deadline.min(handshake_deadline)
        } else {
            request_deadline
        };
        let processing = async {
            if let Err(mut error) = process_message(
                message,
                &mut socket,
                Arc::clone(&startup),
                Arc::clone(&query),
                Arc::clone(&extended),
                Arc::clone(&copy),
                Arc::clone(&cancel),
            )
            .await
            {
                errors.on_error(&socket, &mut error);
                process_error(&mut socket, error, is_extended).await?;
            }
            Ok::<(), io::Error>(())
        };
        tokio::time::timeout_at(request_deadline, processing)
            .await
            .map_err(|_| timed_out("protocol request"))??;
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedCodec, Decoder, PgWireMessageServerCodec};
    use bytes::BytesMut;
    use pgwire::{
        api::{ClientInfo, DefaultClient, PgWireConnectionState},
        error::PgWireError,
    };

    fn make_codec(startup: bool, frame_bytes: usize) -> BoundedCodec {
        let mut client = DefaultClient::<String>::new(([127, 0, 0, 1], 5432).into(), false);
        client.set_state(if startup {
            PgWireConnectionState::AwaitingStartup
        } else {
            PgWireConnectionState::ReadyForQuery
        });
        BoundedCodec {
            inner: PgWireMessageServerCodec::new(client),
            startup,
            frame_bytes,
        }
    }

    #[test]
    fn declared_oversize_is_rejected_with_only_the_header_buffered() {
        let mut codec = make_codec(false, 32);
        let mut bytes = BytesMut::from(&b"Q\x00\x01\x00\x00"[..]);
        assert!(matches!(
            codec.decode(&mut bytes),
            Err(PgWireError::MessageTooLarge(65_536, 32))
        ));
        assert_eq!(bytes.len(), 5);
    }

    #[test]
    fn malformed_lengths_and_partial_headers_do_not_reach_message_parsing() {
        let mut codec = make_codec(false, 32);
        for length in [0_i32, 3, -1] {
            let mut bytes = BytesMut::from(&b"Q"[..]);
            bytes.extend_from_slice(&length.to_be_bytes());
            assert!(codec.decode(&mut bytes).is_err());
        }
        assert!(
            codec
                .decode(&mut BytesMut::from(&b"Q\x00\x00"[..]))
                .is_ok_and(|message| message.is_none())
        );
        let mut startup = make_codec(true, 32);
        let mut bytes = BytesMut::from(&64_i32.to_be_bytes()[..]);
        assert!(matches!(
            startup.decode(&mut bytes),
            Err(PgWireError::MessageTooLarge(64, 32))
        ));
    }

    #[test]
    fn configured_cap_does_not_weaken_upstream_startup_packet_limit() {
        let mut codec = make_codec(true, 32_768);
        let mut bytes = BytesMut::from(&20_000_i32.to_be_bytes()[..]);
        bytes.extend_from_slice(&196_608_i32.to_be_bytes()); // Protocol 3.0.
        assert!(matches!(
            codec.decode(&mut bytes),
            Err(PgWireError::MessageTooLarge(20_000, 10_000))
        ));
    }
}
