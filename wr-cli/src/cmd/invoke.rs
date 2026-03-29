use anyhow::{Context, Result};
use clap::Args;
use reqwest::header::CONTENT_TYPE;

#[derive(Args)]
pub struct InvokeArgs {
    /// Proxy address
    #[arg(long, env = "WR_PROXY", default_value = "http://127.0.0.1:9001")]
    pub proxy: String,

    /// Destination module URL, e.g. http://inventory.ecommerce/seed
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
    let path = extract_path(&args.destination);
    let proxy_url = format!("{}{}", args.proxy.trim_end_matches('/'), path);

    let method = reqwest::Method::from_bytes(args.method.to_uppercase().as_bytes())
        .context("Invalid HTTP method")?;

    let client = reqwest::Client::new();
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

/// Extract the path component from a URL like `http://host/path` → `/path`.
fn extract_path(url: &str) -> &str {
    let after_scheme = url.find("://").map(|i| &url[i + 3..]).unwrap_or(url);
    after_scheme
        .find('/')
        .map(|i| &after_scheme[i..])
        .unwrap_or("/")
}
