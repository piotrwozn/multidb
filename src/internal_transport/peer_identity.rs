//! Explicit membership binding layered on rustls certificate-chain verification.
//! A certificate signed by the cluster CA is not, by itself, an admitted node.

use std::collections::BTreeMap;

use rustls_pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{client::verify_server_name, server::ParsedCertificate};

use super::{
    INTERNAL_ALPN, InternalRequest, InternalTlsConfig, InternalTransportError, load_certs,
};
use crate::repl::NodeId;

#[derive(Clone)]
pub(super) struct PeerIdentities {
    pub(super) local_node: NodeId,
    peers: BTreeMap<NodeId, PeerIdentity>,
}

#[derive(Clone)]
struct PeerIdentity {
    certificate: CertificateDer<'static>,
    server_name: ServerName<'static>,
}

fn rejected(message: impl Into<String>) -> InternalTransportError {
    InternalTransportError::Tls(message.into())
}

impl PeerIdentities {
    pub(super) fn load(config: &InternalTlsConfig) -> Result<Self, InternalTransportError> {
        if config.local_node == 0 {
            return Err(rejected("mTLS local node id must be nonzero"));
        }
        let local = load_certs(&config.cert_path)?.remove(0);
        let mut certificates = BTreeMap::from([(local.as_ref().to_vec(), config.local_node)]);
        let mut peers = BTreeMap::new();
        for (&node, peer) in &config.peers {
            if node == 0 {
                return Err(rejected("mTLS peer node id must be nonzero"));
            }
            let certificate = load_certs(&peer.cert_path)?.remove(0);
            ParsedCertificate::try_from(&certificate)
                .map_err(|error| rejected(format!("invalid mTLS peer certificate: {error}")))?;
            if node == config.local_node && certificate != local {
                return Err(rejected(
                    "mTLS local node pin differs from local certificate",
                ));
            }
            if let Some(previous) = certificates.insert(certificate.as_ref().to_vec(), node)
                && previous != node
            {
                return Err(rejected(
                    "mTLS leaf certificate is assigned to multiple node ids",
                ));
            }
            let server_name = ServerName::try_from(peer.server_name.clone())
                .map_err(|error| rejected(format!("invalid mTLS peer server name: {error}")))?;
            peers.insert(
                node,
                PeerIdentity {
                    certificate,
                    server_name,
                },
            );
        }
        Ok(Self {
            local_node: config.local_node,
            peers,
        })
    }

    pub(super) fn check_local(&self, node: NodeId) -> Result<(), InternalTransportError> {
        if node != self.local_node {
            return Err(rejected(format!(
                "mTLS local certificate configured for node {}, not node {node}",
                self.local_node
            )));
        }
        Ok(())
    }

    pub(super) fn server_name(
        &self,
        node: NodeId,
    ) -> Result<ServerName<'static>, InternalTransportError> {
        self.peers
            .get(&node)
            .map(|peer| peer.server_name.clone())
            .ok_or(InternalTransportError::MissingPeer(node))
    }

    pub(super) fn authenticate(
        &self,
        certificates: Option<&[CertificateDer<'_>]>,
        alpn: Option<&[u8]>,
        expected: Option<NodeId>,
    ) -> Result<NodeId, InternalTransportError> {
        if alpn != Some(INTERNAL_ALPN) {
            return Err(rejected(
                "mTLS peer did not negotiate the internal transport ALPN",
            ));
        }
        let leaf = certificates
            .and_then(|chain| chain.first())
            .ok_or_else(|| rejected("mTLS peer did not present a certificate"))?;
        let (node, identity) = self
            .peers
            .iter()
            .find_map(|(&node, peer)| {
                (peer.certificate.as_ref() == leaf.as_ref()).then_some((node, peer))
            })
            .ok_or_else(|| rejected("mTLS certificate does not identify an admitted peer"))?;
        // Client certificates do not get hostname verification from rustls's
        // client-auth verifier. Apply the admitted SAN name in both directions.
        let parsed = ParsedCertificate::try_from(leaf)
            .map_err(|error| rejected(format!("invalid mTLS peer certificate: {error}")))?;
        verify_server_name(&parsed, &identity.server_name).map_err(|error| {
            rejected(format!(
                "mTLS peer SAN does not match its admitted name: {error}"
            ))
        })?;
        if let Some(expected) = expected
            && expected != node
        {
            return Err(rejected(format!(
                "mTLS peer node {node} differs from requested node {expected}"
            )));
        }
        Ok(node)
    }
}

/// Only request fields that claim a sender must match the authenticated channel.
/// AP versions may legitimately carry other origins during repair/hinted handoff.
pub(super) fn check_request_sender(
    request: &InternalRequest,
    peer: NodeId,
) -> Result<(), InternalTransportError> {
    let from = match request {
        InternalRequest::HealthPing { from } | InternalRequest::FlowAck { from, .. } => Some(*from),
        InternalRequest::Raft(request) => Some(request.sender()),
        InternalRequest::Ap(_) | InternalRequest::ClusterAdmin(_) => None,
    };
    if from.is_some_and(|from| from != peer) {
        return Err(rejected(
            "internal request sender does not match the authenticated mTLS node",
        ));
    }
    Ok(())
}
