//! Statement budgets cover frame ownership before allocation, including a
//! certified peer that advertises a maliciously large response length.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::TcpStream,
};

use super::{
    ApRequest, ApResponse, FRAME_LEN_BYTES, FrameWire, InternalRequest, InternalResponse,
    InternalTransportClient, InternalTransportError, MAX_WIRE_BYTES, check_request_sender,
    encode_frame, response_budget, write_encoded_frame,
};
use crate::{
    repl::{NodeId, ReplError, ap::ApReplicaEndpoint},
    storage::{ReadBudget, ReadReservation, StorageError},
};

pub(super) struct GuardedResponse {
    pub(super) response: InternalResponse,
    pub(super) reservation: Box<dyn ReadReservation>,
}

fn resource(message: impl Into<String>) -> InternalTransportError {
    StorageError::ResourceLimit(message.into()).into()
}

fn timeout_ms(
    configured: u64,
    budget: &dyn ReadBudget,
) -> Result<Duration, InternalTransportError> {
    budget.check()?;
    let configured = Duration::from_millis(configured.max(1));
    Ok(budget.deadline().map_or(configured, |deadline| {
        configured.min(deadline.saturating_duration_since(Instant::now()))
    }))
}

pub(super) async fn read_response<S: AsyncRead + Unpin>(
    stream: &mut S,
    wire_cap: usize,
    timeout: Duration,
    budget: &dyn ReadBudget,
) -> Result<GuardedResponse, InternalTransportError> {
    tokio::time::timeout(timeout, async {
        budget.check()?;
        let length = usize::try_from(stream.read_u32().await?)
            .map_err(|_| resource("response length overflows platform"))?;
        budget.check()?;
        if length > wire_cap || length > budget.remaining_bytes() {
            return Err(resource("AP response length exceeds remaining wire budget"));
        }
        let _wire = budget.reserve(length.saturating_add(2 * std::mem::size_of::<Vec<u8>>()))?;
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await?;
        budget.check()?;
        let decoded =
            InternalResponse::decoded_size(&bytes, budget.remaining_bytes()).map_err(|error| {
                match error {
                    InternalTransportError::Codec(message) if message.contains("allocation") => {
                        resource(message)
                    }
                    other => other,
                }
            })?;
        let reservation = budget.reserve(decoded)?;
        let (response, trailing) = postcard::take_from_bytes(&bytes)
            .map_err(|error| InternalTransportError::Codec(error.to_string()))?;
        if !trailing.is_empty() {
            return Err(InternalTransportError::Codec(
                "trailing guarded AP response bytes".to_owned(),
            ));
        }
        budget.check()?;
        Ok(GuardedResponse {
            response,
            reservation,
        })
    })
    .await
    .map_err(|_| {
        InternalTransportError::Budget(StorageError::Timeout(
            "guarded AP response timed out".to_owned(),
        ))
    })?
}

impl InternalTransportClient {
    pub(super) fn request_guarded(
        &self,
        target: NodeId,
        request: InternalRequest,
        budget: &dyn ReadBudget,
    ) -> Result<GuardedResponse, InternalTransportError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|error| InternalTransportError::Runtime(error.to_string()))?;
                        runtime.block_on(self.request_guarded_async(target, request, budget))
                    })
                    .join()
                    .map_err(|_| {
                        InternalTransportError::Runtime(
                            "guarded transport worker panicked".to_owned(),
                        )
                    })?
            })
        } else {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| InternalTransportError::Runtime(error.to_string()))?;
            runtime.block_on(self.request_guarded_async(target, request, budget))
        }
    }

    async fn request_guarded_async(
        &self,
        target: NodeId,
        request: InternalRequest,
        budget: &dyn ReadBudget,
    ) -> Result<GuardedResponse, InternalTransportError> {
        budget.check()?;
        let address = self
            .peers
            .get(&target)
            .ok_or(InternalTransportError::MissingPeer(target))?;
        if let Some(identities) = &self.identities {
            check_request_sender(&request, identities.local_node)?;
        }
        let cap = self
            .config
            .max_frame_bytes
            .clamp(FRAME_LEN_BYTES, MAX_WIRE_BYTES)
            .min(budget.remaining_bytes());
        let size = response_budget::encoded_size(&request, cap)
            .map_err(|error| resource(error.to_string()))?;
        let request_wire = budget.reserve(size.saturating_add(128))?;
        let bytes = encode_frame(&request, cap)?;
        let _flow = self.reserve(target, bytes.len())?;
        let duration = timeout_ms(self.config.request_timeout_ms, budget)?;
        tokio::time::timeout(duration, async {
            let socket = tokio::time::timeout(
                timeout_ms(self.config.connect_timeout_ms, budget)?,
                TcpStream::connect(address),
            )
            .await
            .map_err(|_| {
                InternalTransportError::Budget(StorageError::Timeout(
                    "guarded AP connect timed out".to_owned(),
                ))
            })??;
            socket.set_nodelay(true)?;
            let mut stream = self.client_stream(socket, target).await?;
            write_encoded_frame(&mut stream, &bytes).await?;
            drop(bytes);
            drop(request_wire);
            let response = read_response(
                &mut stream,
                cap,
                timeout_ms(self.config.idle_timeout_ms, budget)?,
                budget,
            )
            .await?;
            match response.response {
                InternalResponse::Error(message) => Err(InternalTransportError::Remote(message)),
                InternalResponse::ResourceLimit(message) => Err(resource(message)),
                other => Ok(GuardedResponse {
                    response: other,
                    reservation: response.reservation,
                }),
            }
        })
        .await
        .map_err(|_| {
            InternalTransportError::Budget(StorageError::Timeout(
                "guarded AP request timed out".to_owned(),
            ))
        })?
    }
}

pub(super) fn into_repl(error: InternalTransportError) -> ReplError {
    match error {
        InternalTransportError::Budget(error) => error.into(),
        other => ReplError::Transport(other.to_string()),
    }
}

pub(crate) struct PageBudget {
    used: Arc<AtomicUsize>,
    limit: usize,
    deadline: Instant,
}
struct PageReservation {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for PageReservation {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl PageBudget {
    pub(crate) fn new(limit: usize, timeout: Duration) -> Result<Self, StorageError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| StorageError::Timeout("page deadline exceeds clock range".to_owned()))?;
        Ok(Self {
            used: Arc::new(AtomicUsize::new(0)),
            limit,
            deadline,
        })
    }
}
impl ReadBudget for PageBudget {
    fn check(&self) -> Result<(), StorageError> {
        if Instant::now() >= self.deadline {
            Err(StorageError::Timeout(
                "AP page deadline exceeded".to_owned(),
            ))
        } else {
            Ok(())
        }
    }
    fn reserve(&self, bytes: usize) -> Result<Box<dyn ReadReservation>, StorageError> {
        self.check()?;
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used
                .checked_add(bytes)
                .filter(|next| *next <= self.limit)
                .ok_or_else(|| {
                    StorageError::ResourceLimit("AP page exceeds peer read budget".to_owned())
                })?;
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Ok(Box::new(PageReservation {
                        used: Arc::clone(&self.used),
                        bytes,
                    }));
                }
                Err(current) => used = current,
            }
        }
    }
    fn remaining_bytes(&self) -> usize {
        self.limit.saturating_sub(self.used.load(Ordering::Acquire))
    }
    fn deadline(&self) -> Option<Instant> {
        Some(self.deadline)
    }
}

pub(super) struct ServedResponse {
    pub(super) response: InternalResponse,
    pub(super) reservation: Option<Box<dyn ReadReservation>>,
    pub(super) wire_cap: usize,
}

pub(super) fn page_response(
    request: ApRequest,
    endpoint: &dyn ApReplicaEndpoint,
    server_wire_cap: usize,
    timeout: Duration,
) -> Result<ServedResponse, ReplError> {
    let ApRequest::ReadRangePageGuarded {
        table,
        start,
        end,
        limit,
        max_wire_bytes,
        max_decoded_bytes,
    } = request
    else {
        return Err(ReplError::Rejected(
            "expected a guarded page request".to_owned(),
        ));
    };
    let wire_cap = max_wire_bytes.min(server_wire_cap);
    if wire_cap < FRAME_LEN_BYTES || max_decoded_bytes == 0 {
        return Err(StorageError::ResourceLimit(
            "AP peer supplied an empty response budget".to_owned(),
        )
        .into());
    }
    let budget = PageBudget::new(
        max_decoded_bytes.min(super::frame_budget::MAX_DECODED_BYTES),
        timeout,
    )?;
    let page = endpoint.read_range_page_guarded(&table, &start, &end, limit, &budget)?;
    let response = InternalResponse::Ap(ApResponse::Records(page.records));
    let size = response_budget::encoded_size(&response, wire_cap)
        .map_err(|error| StorageError::ResourceLimit(error.to_string()))?;
    let wire = budget.reserve(size.saturating_add(128))?;
    let reservations = vec![page.reservation, wire];
    Ok(ServedResponse {
        response,
        reservation: Some(Box::new(reservations)),
        wire_cap,
    })
}

#[cfg(test)]
mod tests;
