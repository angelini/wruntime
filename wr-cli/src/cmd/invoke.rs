use anyhow::{Context, Result};
use clap::Args;
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage};
use reqwest::header::CONTENT_TYPE;

use wr_common::wruntime::{GetSchemaRequest, ListEnginesRequest};

#[derive(Args)]
pub struct InvokeArgs {
    /// Proxy address
    #[arg(long, env = "WR_PROXY", default_value = "http://127.0.0.1:9001")]
    pub proxy: String,

    /// Destination module URL, e.g. http://ecommerce.inventory/Seed
    #[arg(long)]
    pub destination: String,

    /// Source module name attached as x-wr-source
    #[arg(long, default_value = "wr-cli")]
    pub source: String,

    /// Source namespace attached as x-wr-source-ns
    #[arg(long, default_value = "default")]
    pub source_ns: String,

    /// HTTP method
    #[arg(long, default_value = "POST")]
    pub method: String,

    /// Request body (JSON — transcoded to protobuf using the module's schema)
    #[arg(long)]
    pub body: Option<String>,
}

pub async fn run(args: InvokeArgs, manager_addr: &str) -> Result<()> {
    let proxy_url = build_proxy_url(&args.proxy, &args.destination)?;

    let dest: reqwest::Url = args
        .destination
        .parse()
        .context("invalid destination URL")?;
    let host = dest.host_str().unwrap_or("");
    let path = dest.path();

    // Parse "{namespace}.{module}" from the destination host.
    let (namespace, module) = host
        .split_once('.')
        .context("destination host must be {namespace}.{module}")?;

    // Fetch the module's schema from the manager and transcode JSON → protobuf.
    let body_bytes = match &args.body {
        Some(json_str) => {
            let schema_bytes = fetch_schema(manager_addr, namespace, module).await?;
            transcode_json_to_proto(&schema_bytes, path, json_str.as_bytes())?
        }
        None => Vec::new(),
    };

    let method = reqwest::Method::from_bytes(args.method.to_uppercase().as_bytes())
        .context("Invalid HTTP method")?;

    let client = reqwest::Client::builder().http2_prior_knowledge().build()?;
    let resp = client
        .request(method, &proxy_url)
        .header(CONTENT_TYPE, "application/protobuf")
        .header("x-wr-destination", &args.destination)
        .header("x-wr-source", &args.source)
        .header("x-wr-source-ns", &args.source_ns)
        .body(body_bytes)
        .send()
        .await
        .with_context(|| format!("Request to {proxy_url} failed"))?;

    let status = resp.status();
    let body = resp.text().await?;

    println!("HTTP {status}");
    if !body.is_empty() {
        println!("{body}");
    }

    if !status.is_success() {
        anyhow::bail!("Request failed with status {status}");
    }

    Ok(())
}

/// Fetch the `FileDescriptorSet` bytes for a module from the manager.
///
/// The manager requires an exact `(namespace, module, version)` triple, so we
/// first list registered engines to discover the version for this module.
async fn fetch_schema(manager_addr: &str, namespace: &str, module: &str) -> Result<Vec<u8>> {
    let mut client = crate::client::connect(manager_addr).await?;

    // Discover the version by scanning registered engines.
    let engines = client
        .list_engines(ListEnginesRequest {})
        .await
        .context("failed to list engines")?
        .into_inner()
        .engines;

    let version = engines
        .iter()
        .flat_map(|e| &e.modules)
        .find(|m| m.namespace == namespace && m.name == module)
        .map(|m| m.version.clone())
        .with_context(|| format!("no registered module '{namespace}.{module}' found"))?;

    let resp = client
        .get_schema(GetSchemaRequest {
            namespace: namespace.into(),
            module: module.into(),
            version,
        })
        .await
        .context("failed to fetch schema from manager")?;
    Ok(resp.into_inner().proto_schema)
}

/// Transcode a JSON body to protobuf using the module's schema.
///
/// `path` is the RPC method path (e.g. `/Seed`). The method name is extracted
/// from the final path segment, looked up in the schema's services, and the
/// input message descriptor is used to parse the JSON body.
///
/// An empty JSON body (`""`, `"{}"`) produces an empty protobuf message (zero bytes).
fn transcode_json_to_proto(schema_bytes: &[u8], path: &str, json_body: &[u8]) -> Result<Vec<u8>> {
    let pool = DescriptorPool::decode(schema_bytes)
        .context("failed to decode FileDescriptorSet from manager")?;

    let method_name = path
        .trim_start_matches('/')
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .context("path must contain a method name (e.g. /Seed)")?;

    let input_desc = pool
        .services()
        .find_map(|s| s.methods().find(|m| m.name() == method_name))
        .map(|m| m.input())
        .with_context(|| format!("method '{method_name}' not found in schema"))?;

    // Empty or whitespace-only body → empty proto3 message (zero bytes).
    let trimmed = json_body
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace());
    if trimmed.clone().count() == 0 {
        return Ok(Vec::new());
    }

    // A body of just `''` (empty string from CLI) also maps to an empty message.
    if json_body == b"''" || json_body == b"\"\"" {
        return Ok(Vec::new());
    }

    let mut de = serde_json::Deserializer::from_slice(json_body);
    let dynamic_msg = DynamicMessage::deserialize(input_desc, &mut de)
        .context("failed to transcode JSON body to protobuf")?;
    Ok(dynamic_msg.encode_to_vec())
}

/// Build the proxy URL for a destination like `http://ecommerce.inventory/Seed`.
fn build_proxy_url(proxy: &str, destination: &str) -> Result<String> {
    let dest: reqwest::Url = destination
        .parse()
        .with_context(|| format!("invalid destination URL: {destination}"))?;
    let host = dest.host_str().unwrap_or("");
    let path_and_query = match dest.query() {
        Some(q) => format!("{}?{q}", dest.path()),
        None => dest.path().to_string(),
    };
    Ok(format!(
        "{}/{host}{path_and_query}",
        proxy.trim_end_matches('/')
    ))
}
