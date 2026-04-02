use anyhow::Result;
use clap::{Args, Subcommand};
use tabled::builder::Builder;
use wr_common::wruntime::{DeleteSecretRequest, ListSecretsRequest, SetSecretRequest};

use crate::{client, display};

#[derive(Args)]
pub struct SecretsArgs {
    #[command(subcommand)]
    pub command: SecretsCommand,
}

#[derive(Subcommand)]
pub enum SecretsCommand {
    /// Set a secret value for a namespace
    Set {
        /// Namespace (e.g. "ecommerce")
        namespace: String,
        /// Secret key (e.g. "API_KEY")
        key: String,
        /// Secret value
        value: String,
    },
    /// Delete a secret
    Delete {
        /// Namespace
        namespace: String,
        /// Secret key
        key: String,
    },
    /// List secrets (keys only, no values)
    List {
        /// Filter by namespace
        #[arg(long)]
        namespace: Option<String>,
    },
}

pub async fn run(args: SecretsArgs, manager: &str) -> Result<()> {
    match args.command {
        SecretsCommand::Set {
            namespace,
            key,
            value,
        } => set(manager, &namespace, &key, &value).await,
        SecretsCommand::Delete { namespace, key } => delete(manager, &namespace, &key).await,
        SecretsCommand::List { namespace } => list(manager, namespace.as_deref()).await,
    }
}

async fn set(manager: &str, namespace: &str, key: &str, value: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    client
        .set_secret(SetSecretRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value: value.to_string(),
        })
        .await?;
    println!("Secret '{namespace}/{key}' stored.");
    Ok(())
}

async fn delete(manager: &str, namespace: &str, key: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    client
        .delete_secret(DeleteSecretRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
        })
        .await?;
    println!("Secret '{namespace}/{key}' deleted.");
    Ok(())
}

async fn list(manager: &str, namespace: Option<&str>) -> Result<()> {
    let mut client = client::connect(manager).await?;
    let resp = client
        .list_secrets(ListSecretsRequest {
            namespace: namespace.unwrap_or_default().to_string(),
        })
        .await?
        .into_inner();

    if resp.secrets.is_empty() {
        println!("No secrets found.");
        return Ok(());
    }

    let mut builder = Builder::new();
    builder.push_record(["Namespace", "Key"]);
    for entry in &resp.secrets {
        builder.push_record([entry.namespace.as_str(), entry.key.as_str()]);
    }
    display::print_table(builder);
    Ok(())
}
