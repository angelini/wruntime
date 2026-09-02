use std::collections::HashMap;

use tonic::{Request, Status};

use crate::config::{PrincipalMapping, PrincipalRole};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedPrincipal {
    pub name: String,
    pub role: PrincipalRole,
    pub node_id: Option<String>,
}

#[derive(Clone, Default)]
pub struct PrincipalPolicy {
    by_fingerprint: HashMap<String, AuthorizedPrincipal>,
}

impl PrincipalPolicy {
    pub fn new(mappings: &[PrincipalMapping]) -> Self {
        let by_fingerprint = mappings
            .iter()
            .map(|mapping| {
                (
                    mapping.fingerprint.clone(),
                    AuthorizedPrincipal {
                        name: mapping.principal.trim().to_string(),
                        role: mapping.role,
                        node_id: mapping.node_id.clone(),
                    },
                )
            })
            .collect();
        Self { by_fingerprint }
    }

    fn peer<T>(&self, request: &Request<T>) -> Result<AuthorizedPrincipal, Status> {
        let certificate = request
            .peer_certs()
            .and_then(|certificates| certificates.first().cloned())
            .ok_or_else(|| Status::unauthenticated("client certificate identity is unavailable"))?;
        let fingerprint = wr_common::tls::certificate_fingerprint_sha256(certificate.as_ref());
        self.by_fingerprint
            .get(&fingerprint)
            .cloned()
            .ok_or_else(|| {
                Status::permission_denied(
                    "client certificate is not mapped to an operator principal",
                )
            })
    }

    pub fn authorize_read<T>(
        &self,
        request: &mut Request<T>,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        if !matches!(
            principal.role,
            PrincipalRole::Viewer | PrincipalRole::Operator
        ) {
            return Err(Status::permission_denied(
                "viewer or operator role is required",
            ));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }

    pub fn authorize_operator<T>(
        &self,
        request: &mut Request<T>,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        if principal.role != PrincipalRole::Operator {
            return Err(Status::permission_denied("operator role is required"));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }

    pub fn authorize_agent<T>(
        &self,
        request: &mut Request<T>,
        requested_node_id: &str,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        if principal.role != PrincipalRole::NodeAgent
            || principal.node_id.as_deref() != Some(requested_node_id)
        {
            return Err(Status::permission_denied(
                "node-agent principal is not bound to the requested node",
            ));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_rotation_maps_to_one_identity() {
        let policy = PrincipalPolicy::new(&[
            PrincipalMapping {
                fingerprint: format!("sha256:{}", "a".repeat(64)),
                principal: "ops".into(),
                role: PrincipalRole::Operator,
                node_id: None,
            },
            PrincipalMapping {
                fingerprint: format!("sha256:{}", "b".repeat(64)),
                principal: "ops".into(),
                role: PrincipalRole::Operator,
                node_id: None,
            },
        ]);
        assert_eq!(policy.by_fingerprint.len(), 2);
        assert!(policy
            .by_fingerprint
            .values()
            .all(|principal| principal.name == "ops"));
    }
}
