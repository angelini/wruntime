use anyhow::{Context, Result};
use clap::Args;
use reqwest::header::CONTENT_TYPE;

#[derive(Args)]
pub struct InvokeArgs {
    /// Proxy address
    #[arg(long, env = "WR_PROXY", default_value = "http://127.0.0.1:9001")]
    pub proxy: String,

    /// Destination module URL, e.g. http://ecommerce.inventory/seed
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

    /// Request body
    #[arg(long)]
    pub body: Option<String>,

    /// Content-Type header
    #[arg(long, default_value = "application/json")]
    pub content_type: String,
}

pub async fn run(args: InvokeArgs) -> Result<()> {
    let proxy_url = build_proxy_url(&args.proxy, &args.destination)?;

    let method = reqwest::Method::from_bytes(args.method.to_uppercase().as_bytes())
        .context("Invalid HTTP method")?;

    let client = reqwest::Client::builder().http2_prior_knowledge().build()?;
    let mut req = client
        .request(method, &proxy_url)
        .header(CONTENT_TYPE, &args.content_type)
        .header("x-wr-destination", &args.destination)
        .header("x-wr-source", &args.source)
        .header("x-wr-source-ns", &args.source_ns);

    if let Some(body) = args.body {
        req = req.body(body);
    }

    let resp = req
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

/// Build the proxy URL for a destination like `http://ecommerce.inventory/Seed`.
///
/// The proxy URL uses `/{host}{path}` so the engine receives the full
/// `/{namespace}.{module}/{method}` path that WASM module handlers match against.
/// For example: `http://127.0.0.1:9001/ecommerce.inventory/Seed`.
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
