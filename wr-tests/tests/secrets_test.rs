#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;

use wr_common::wruntime::{
    DeleteSecretRequest, EngineRegistration, ListSecretsRequest, ModuleDescriptor,
    RegisterEngineRequest, SecretRequest, SetSecretRequest,
};

#[tokio::test]
async fn test_set_and_list_secrets() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Set two secrets in the same namespace.
    c.set_secret(SetSecretRequest {
        namespace: "payments".into(),
        key: "STRIPE_KEY".into(),
        value: "sk_test_abc123".into(),
    })
    .await?;
    c.set_secret(SetSecretRequest {
        namespace: "payments".into(),
        key: "WEBHOOK_SECRET".into(),
        value: "whsec_xyz789".into(),
    })
    .await?;

    // Set a secret in a different namespace.
    c.set_secret(SetSecretRequest {
        namespace: "auth".into(),
        key: "JWT_SECRET".into(),
        value: "super-secret-jwt".into(),
    })
    .await?;

    // List all secrets — should see all 3 entries (metadata only, no values).
    let resp = c
        .list_secrets(ListSecretsRequest {
            namespace: String::new(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.secrets.len(), 3);

    // List by namespace — should see only the 2 payments secrets.
    let resp = c
        .list_secrets(ListSecretsRequest {
            namespace: "payments".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.secrets.len(), 2);
    assert!(resp.secrets.iter().all(|s| s.namespace == "payments"));
    let keys: Vec<&str> = resp.secrets.iter().map(|s| s.key.as_str()).collect();
    assert!(keys.contains(&"STRIPE_KEY"));
    assert!(keys.contains(&"WEBHOOK_SECRET"));

    // List by namespace with no secrets — should return empty.
    let resp = c
        .list_secrets(ListSecretsRequest {
            namespace: "nonexistent".into(),
        })
        .await?
        .into_inner();
    assert!(resp.secrets.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_delete_secret() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    c.set_secret(SetSecretRequest {
        namespace: "ns".into(),
        key: "KEY".into(),
        value: "val".into(),
    })
    .await?;

    // Verify it exists.
    let resp = c
        .list_secrets(ListSecretsRequest {
            namespace: "ns".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.secrets.len(), 1);

    // Delete it.
    c.delete_secret(DeleteSecretRequest {
        namespace: "ns".into(),
        key: "KEY".into(),
    })
    .await?;

    // Verify it's gone.
    let resp = c
        .list_secrets(ListSecretsRequest {
            namespace: "ns".into(),
        })
        .await?
        .into_inner();
    assert!(resp.secrets.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_set_secret_upsert_overwrites() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    c.set_secret(SetSecretRequest {
        namespace: "ns".into(),
        key: "API_KEY".into(),
        value: "old-value".into(),
    })
    .await?;

    // Overwrite with a new value.
    c.set_secret(SetSecretRequest {
        namespace: "ns".into(),
        key: "API_KEY".into(),
        value: "new-value".into(),
    })
    .await?;

    // Should still be exactly one secret, not two.
    let resp = c
        .list_secrets(ListSecretsRequest {
            namespace: "ns".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.secrets.len(), 1);

    // Verify the new value is returned during registration.
    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    let reg_resp = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "upsert-engine".into(),
                address: engine_addr,
                proxy_address: "http://127.0.0.1:9001".into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![ModuleDescriptor {
                    name: "mod".into(),
                    namespace: "ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                }],
                secrets: vec![SecretRequest {
                    namespace: "ns".into(),
                    key: "API_KEY".into(),
                }],
                db_namespaces: vec![],
            }),
        })
        .await?
        .into_inner();

    assert!(reg_resp.accepted);
    assert_eq!(reg_resp.secrets.len(), 1);
    assert_eq!(reg_resp.secrets[0].namespace, "ns");
    assert_eq!(
        reg_resp.secrets[0].secrets.get("API_KEY").unwrap(),
        "new-value"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_set_secret_empty_namespace_rejected() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let result = c
        .set_secret(SetSecretRequest {
            namespace: String::new(),
            key: "KEY".into(),
            value: "val".into(),
        })
        .await;
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn test_set_secret_empty_key_rejected() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let result = c
        .set_secret(SetSecretRequest {
            namespace: "ns".into(),
            key: String::new(),
            value: "val".into(),
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn test_delete_secret_empty_fields_rejected() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let result = c
        .delete_secret(DeleteSecretRequest {
            namespace: String::new(),
            key: "KEY".into(),
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    let result = c
        .delete_secret(DeleteSecretRequest {
            namespace: "ns".into(),
            key: String::new(),
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn test_register_engine_with_secrets() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Store secrets.
    c.set_secret(SetSecretRequest {
        namespace: "myapp".into(),
        key: "DB_PASSWORD".into(),
        value: "hunter2".into(),
    })
    .await?;
    c.set_secret(SetSecretRequest {
        namespace: "myapp".into(),
        key: "API_TOKEN".into(),
        value: "tok_abc".into(),
    })
    .await?;

    // Register engine requesting those secrets.
    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    let resp = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "secret-engine".into(),
                address: engine_addr,
                proxy_address: "http://127.0.0.1:9001".into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![ModuleDescriptor {
                    name: "svc".into(),
                    namespace: "myapp".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                }],
                secrets: vec![
                    SecretRequest {
                        namespace: "myapp".into(),
                        key: "DB_PASSWORD".into(),
                    },
                    SecretRequest {
                        namespace: "myapp".into(),
                        key: "API_TOKEN".into(),
                    },
                ],
                db_namespaces: vec![],
            }),
        })
        .await?
        .into_inner();

    assert!(resp.accepted);
    // Should have one NamespaceSecrets entry for "myapp".
    assert_eq!(resp.secrets.len(), 1);
    let ns_secrets = &resp.secrets[0];
    assert_eq!(ns_secrets.namespace, "myapp");
    assert_eq!(ns_secrets.secrets.len(), 2);
    assert_eq!(ns_secrets.secrets.get("DB_PASSWORD").unwrap(), "hunter2");
    assert_eq!(ns_secrets.secrets.get("API_TOKEN").unwrap(), "tok_abc");

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_register_engine_with_missing_secret_fails() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Register engine requesting a secret that doesn't exist.
    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    let result = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "missing-secret-engine".into(),
                address: engine_addr,
                proxy_address: "http://127.0.0.1:9001".into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![ModuleDescriptor {
                    name: "svc".into(),
                    namespace: "myapp".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                }],
                secrets: vec![SecretRequest {
                    namespace: "myapp".into(),
                    key: "NONEXISTENT".into(),
                }],
                db_namespaces: vec![],
            }),
        })
        .await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains("missing secrets"));

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_register_engine_no_secrets_succeeds() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    let resp = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "no-secrets-engine".into(),
                address: engine_addr,
                proxy_address: "http://127.0.0.1:9001".into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![ModuleDescriptor {
                    name: "svc".into(),
                    namespace: "ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                }],
                secrets: vec![],
                db_namespaces: vec![],
            }),
        })
        .await?
        .into_inner();

    assert!(resp.accepted);
    assert!(resp.secrets.is_empty());

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_secrets_across_namespaces() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Store secrets in two namespaces.
    c.set_secret(SetSecretRequest {
        namespace: "frontend".into(),
        key: "API_KEY".into(),
        value: "fe-key".into(),
    })
    .await?;
    c.set_secret(SetSecretRequest {
        namespace: "backend".into(),
        key: "API_KEY".into(),
        value: "be-key".into(),
    })
    .await?;

    // Register engine requesting secrets from both namespaces.
    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    let resp = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "multi-ns-engine".into(),
                address: engine_addr,
                proxy_address: "http://127.0.0.1:9001".into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![
                    ModuleDescriptor {
                        name: "fe".into(),
                        namespace: "frontend".into(),
                        version: "1.0.0".into(),
                        proto_schema: minimal_file_descriptor_set(),
                    },
                    ModuleDescriptor {
                        name: "be".into(),
                        namespace: "backend".into(),
                        version: "1.0.0".into(),
                        proto_schema: minimal_file_descriptor_set(),
                    },
                ],
                secrets: vec![
                    SecretRequest {
                        namespace: "frontend".into(),
                        key: "API_KEY".into(),
                    },
                    SecretRequest {
                        namespace: "backend".into(),
                        key: "API_KEY".into(),
                    },
                ],
                db_namespaces: vec![],
            }),
        })
        .await?
        .into_inner();

    assert!(resp.accepted);
    assert_eq!(resp.secrets.len(), 2);

    // Find each namespace's secrets.
    let fe = resp
        .secrets
        .iter()
        .find(|s| s.namespace == "frontend")
        .expect("frontend secrets");
    assert_eq!(fe.secrets.get("API_KEY").unwrap(), "fe-key");

    let be = resp
        .secrets
        .iter()
        .find(|s| s.namespace == "backend")
        .expect("backend secrets");
    assert_eq!(be.secrets.get("API_KEY").unwrap(), "be-key");

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_delete_nonexistent_secret_succeeds() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Deleting a secret that was never set should not error.
    c.delete_secret(DeleteSecretRequest {
        namespace: "ns".into(),
        key: "NEVER_SET".into(),
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_secret_deleted_then_registration_fails() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Set then delete a secret.
    c.set_secret(SetSecretRequest {
        namespace: "ns".into(),
        key: "TEMP".into(),
        value: "val".into(),
    })
    .await?;
    c.delete_secret(DeleteSecretRequest {
        namespace: "ns".into(),
        key: "TEMP".into(),
    })
    .await?;

    // Now register requesting that deleted secret — should fail.
    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    let result = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "deleted-secret-engine".into(),
                address: engine_addr,
                proxy_address: "http://127.0.0.1:9001".into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![ModuleDescriptor {
                    name: "svc".into(),
                    namespace: "ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                }],
                secrets: vec![SecretRequest {
                    namespace: "ns".into(),
                    key: "TEMP".into(),
                }],
                db_namespaces: vec![],
            }),
        })
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_concurrent_db_credential_registration_same_password() -> Result<()> {
    let (_pool, _addr, c) = manager_trio().await?;

    const N: usize = 8;
    let namespace = "concurrent-db-ns";

    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let mut client = c.clone();
        let ns = namespace.to_string();
        handles.push(tokio::spawn(async move {
            client
                .register_engine(RegisterEngineRequest {
                    registration: Some(EngineRegistration {
                        engine_id: format!("db-race-engine-{i}"),
                        address: "http://127.0.0.1:9999".into(),
                        proxy_address: "http://127.0.0.1:9001".into(),
                        peer_address: TEST_SELF_PEER.into(),
                        modules: vec![],
                        secrets: vec![],
                        db_namespaces: vec![ns],
                    }),
                })
                .await
                .map(|r| r.into_inner())
        }));
    }

    let mut passwords = Vec::with_capacity(N);
    for h in handles {
        let resp = h.await.expect("task panicked")?;
        assert!(resp.accepted);
        assert_eq!(resp.db_credentials.len(), 1);
        assert_eq!(resp.db_credentials[0].namespace, namespace);
        passwords.push(resp.db_credentials[0].password.clone());
    }

    let first = &passwords[0];
    assert!(!first.is_empty(), "db password should not be empty");
    assert!(
        passwords.iter().all(|p| p == first),
        "all concurrent registrations must return the same db password"
    );

    // Fast-path read after the race must match the persisted password.
    let mut client = c.clone();
    let resp = client
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "db-race-engine-after".into(),
                address: "http://127.0.0.1:9999".into(),
                proxy_address: "http://127.0.0.1:9001".into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![],
                secrets: vec![],
                db_namespaces: vec![namespace.into()],
            }),
        })
        .await?
        .into_inner();
    assert_eq!(&resp.db_credentials[0].password, first);

    Ok(())
}

#[tokio::test]
async fn test_db_credential_reregistration_returns_same_password() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;
    let namespace = "reg-db-ns";

    let first = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "reg-engine-1".into(),
                address: "http://127.0.0.1:9999".into(),
                proxy_address: "http://127.0.0.1:9001".into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![],
                secrets: vec![],
                db_namespaces: vec![namespace.into()],
            }),
        })
        .await?
        .into_inner();

    let second = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "reg-engine-2".into(),
                address: "http://127.0.0.1:9999".into(),
                proxy_address: "http://127.0.0.1:9001".into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![],
                secrets: vec![],
                db_namespaces: vec![namespace.into()],
            }),
        })
        .await?
        .into_inner();

    assert_eq!(first.db_credentials.len(), 1);
    assert_eq!(second.db_credentials.len(), 1);
    assert!(!first.db_credentials[0].password.is_empty());
    assert_eq!(
        first.db_credentials[0].password,
        second.db_credentials[0].password
    );

    Ok(())
}

#[tokio::test]
async fn test_set_secret_reserved_db_password_key_rejected() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let result = c
        .set_secret(SetSecretRequest {
            namespace: "ns".into(),
            key: "__db_password".into(),
            value: "hacked".into(),
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    Ok(())
}
