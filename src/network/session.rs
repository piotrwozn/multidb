//! Session lifecycle bounds around pgwire's TLS, authentication and protocol dispatch.

use std::{io, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::StreamExt;
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, ErrorHandler, PgWireConnectionState, PgWireServerHandlers,
        portal::PortalExecutionState, store::PortalStore,
    },
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
        if self.startup && length > 10_000 {
            return Err(PgWireError::MessageTooLarge(length, 10_000));
        }
        let total = length.checked_add(offset).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid pgwire frame length")
        })?;
        if bytes.len() < total {
            return Ok(None);
        }
        super::frame::validate(&bytes[offset + 4..total], self.startup, bytes[0])?;
        // The upstream decoder receives exactly one declared frame. It cannot
        // satisfy a malformed field by consuming bytes from the next request.
        let mut packet = bytes.split_to(total);
        let decoded = self.inner.decode(&mut packet)?;
        if decoded.is_none() || !packet.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pgwire frame was not consumed exactly",
            )
            .into());
        }
        Ok(decoded)
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

fn prepare_state_budget<C: ClientInfo + ClientPortalStore>(
    socket: &mut C,
    budget: &super::session_state::Budget,
    message: &PgWireFrontendMessage,
    ready: bool,
) -> io::Result<(Option<super::session_state::Update>, Option<String>)>
where
    C::PortalStore: PortalStore<Statement = String>,
{
    let execution = match message {
        PgWireFrontendMessage::Execute(execute) if ready => Some(
            execute
                .name
                .as_deref()
                .unwrap_or(pgwire::api::DEFAULT_NAME)
                .to_owned(),
        ),
        _ => None,
    };
    // Server-owned fields override any untrusted startup metadata.
    socket
        .metadata_mut()
        .remove(super::session_state::NEW_RESULT_BYTES);
    if execution.is_some() {
        socket.metadata_mut().insert(
            super::session_state::AVAILABLE_RESULT_BYTES.to_owned(),
            budget.available_result_bytes().to_string(),
        );
    }
    let update = if ready {
        budget.prepare(message, socket.portal_store())?
    } else {
        None
    };
    Ok((update, execution))
}

async fn finish_state_budget<C: ClientInfo + ClientPortalStore + Sync>(
    socket: &C,
    budget: &mut super::session_state::Budget,
    execution: Option<&str>,
) -> io::Result<()>
where
    C::PortalStore: PortalStore<Statement = String>,
{
    if let Some(name) = execution {
        let retained = if let Some(portal) = socket.portal_store().get_portal(name) {
            let state = portal.state();
            let suspended = matches!(*state.lock().await, PortalExecutionState::Suspended(_));
            suspended
        } else {
            false
        };
        if retained {
            if let Some(bytes) = socket
                .metadata()
                .get(super::session_state::NEW_RESULT_BYTES)
                .and_then(|bytes| bytes.parse::<usize>().ok())
            {
                budget.record_result(name, Some(bytes))?;
            }
        } else {
            budget.record_result(name, None)?;
        }
    }
    Ok(())
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
    let mut state_budget = super::session_state::Budget::default();
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
        let (state_update, execution) = prepare_state_budget(
            &mut socket,
            &state_budget,
            &message,
            matches!(state, PgWireConnectionState::ReadyForQuery),
        )?;
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
            } else if let Some(update) = state_update {
                state_budget.commit(update);
            }
            Ok::<(), io::Error>(())
        };
        tokio::time::timeout_at(request_deadline, processing)
            .await
            .map_err(|_| timed_out("protocol request"))??;
        finish_state_budget(&socket, &mut state_budget, execution.as_deref()).await?;
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
        client.set_state(PgWireConnectionState::AwaitingStartup);
        let mut inner = PgWireMessageServerCodec::new(client);
        if !startup {
            // The codec's private decode context is initialized by startup in
            // production; setting only client_info.state leaves it awaiting TLS.
            let mut message = BytesMut::from(&b"\0\0\0\x10\0\x03\0\0user\0a\0\0"[..]);
            assert!(inner.decode(&mut message).is_ok_and(|message| {
                matches!(
                    message,
                    Some(pgwire::messages::PgWireFrontendMessage::Startup(_))
                )
            }));
            inner
                .client_info
                .set_state(PgWireConnectionState::ReadyForQuery);
        }
        BoundedCodec {
            inner,
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

    #[test]
    fn malformed_packet_cannot_consume_a_pipelined_next_request() {
        let mut codec = make_codec(false, 1_024);
        let mut bytes = BytesMut::from(&b"Q\0\0\0\x05xQ\0\0\0\x0dselect 1\0"[..]);
        assert!(codec.decode(&mut bytes).is_err());
        let mut codec = make_codec(false, 1_024);
        let mut bytes = BytesMut::from(&b"P\0\0\0\x06\0\0"[..]);
        assert!(codec.decode(&mut bytes).is_err());
        let mut codec = make_codec(false, 1_024);
        let mut bytes = BytesMut::from(&b"B\0\0\0\x0c\0\0\0\0\0\0\xff\xff"[..]);
        assert!(codec.decode(&mut bytes).is_err());
    }

    #[test]
    fn valid_pipelined_requests_preserve_frame_boundaries() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut codec = make_codec(false, 1_024);
        let mut bytes = BytesMut::from(&b"Q\0\0\0\x0dselect 1\0Q\0\0\0\x0dselect 2\0"[..]);
        assert!(
            matches!(codec.decode(&mut bytes)?, Some(pgwire::messages::PgWireFrontendMessage::Query(query)) if query.query == "select 1")
        );
        assert!(
            matches!(codec.decode(&mut bytes)?, Some(pgwire::messages::PgWireFrontendMessage::Query(query)) if query.query == "select 2")
        );
        assert!(bytes.is_empty());
        Ok(())
    }
}
