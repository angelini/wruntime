#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use serde::{Deserialize, Serialize};
use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::bindings::wruntime::blobstore::store;
use wr_sdk::io::{read_body, send_response};
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

const BUCKET: &str = "codegen";

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());
        let (status, resp) = proto::collector_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
    }
}

// ── Manifest stored alongside docs in blobstore ─────────────────────────────

#[derive(Serialize, Deserialize)]
struct Manifest {
    source_type: String,
    owner: String,
    repo: String,
    ref_or_ver: String,
    files: Vec<ManifestEntry>,
    total_bytes: u64,
}

#[derive(Serialize, Deserialize)]
struct ManifestEntry {
    key: String,
    size: u64,
}

// ── Service implementation ───────────────────────────────────────────────────

impl proto::CollectorService for Component {
    fn fetch_docs(
        &self,
        req: proto::FetchDocsRequest,
    ) -> Result<proto::FetchDocsResponse, ServiceError> {
        let mut sources_fetched: i32 = 0;
        let mut total_bytes: i64 = 0;
        let mut doc_prefixes = Vec::new();

        for source in &req.sources {
            let prefix = doc_prefix(source);
            let manifest_key = format!("{prefix}/manifest.json");

            // Check if already fetched (idempotent).
            if store::head_object(BUCKET, &manifest_key).is_ok() {
                wr_sdk::log::log(&format!("skipping already-fetched: {prefix}"));
                doc_prefixes.push(prefix);
                continue;
            }

            let bytes = match source.source_type.as_str() {
                "github_tarball" => fetch_github_tarball(source, &prefix)?,
                "docs_rs" => fetch_docs_rs(source, &prefix)?,
                "crates_io" => fetch_crates_io(source, &prefix)?,
                other => {
                    return Err(ServiceError::bad_request(format!(
                        "unknown source_type: {other}"
                    )))
                }
            };

            total_bytes += bytes as i64;
            sources_fetched += 1;
            doc_prefixes.push(prefix);
        }

        Ok(proto::FetchDocsResponse {
            sources_fetched,
            total_bytes,
            doc_prefixes,
        })
    }

    fn list_docs(
        &self,
        req: proto::ListDocsRequest,
    ) -> Result<proto::ListDocsResponse, ServiceError> {
        let objects = store::list_objects(BUCKET, Some(&req.doc_prefix))
            .map_err(|e| ServiceError::internal(format!("list_objects: {e:?}")))?;

        let chunks = objects
            .into_iter()
            .map(|obj| proto::DocChunkMeta {
                key: obj.key,
                size: obj.size as i64,
                label: String::new(),
            })
            .collect();

        Ok(proto::ListDocsResponse { chunks })
    }
}

// ── HTTP egress helper ───────────────────────────────────────────────────────

fn http_get(url: &str) -> Result<(u16, Vec<u8>), String> {
    use wr_sdk::bindings::wasi::http::{
        outgoing_handler,
        types::{Fields, Method, OutgoingBody, OutgoingRequest, Scheme},
    };
    use wr_sdk::bindings::wasi::io::streams::StreamError;

    // Parse URL: scheme://authority/path
    let (scheme, rest) = if url.starts_with("https://") {
        (Scheme::Https, &url[8..])
    } else if url.starts_with("http://") {
        (Scheme::Http, &url[7..])
    } else {
        return Err(format!("unsupported URL scheme: {url}"));
    };

    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };

    let headers = Fields::new();
    headers
        .set("user-agent", &[b"wruntime-collector/1.0".to_vec()])
        .map_err(|e| format!("set header: {e:?}"))?;

    let req = OutgoingRequest::new(headers);
    req.set_method(&Method::Get).map_err(|_| "set method")?;
    req.set_scheme(Some(&scheme)).map_err(|_| "set scheme")?;
    req.set_authority(Some(authority))
        .map_err(|_| "set authority")?;
    req.set_path_with_query(Some(path))
        .map_err(|_| "set path")?;

    let outgoing_body = req.body().map_err(|_| "get body")?;
    OutgoingBody::finish(outgoing_body, None).map_err(|e| format!("finish body: {e:?}"))?;

    let future_resp =
        outgoing_handler::handle(req, None).map_err(|e| format!("handle: {e:?}"))?;

    loop {
        match future_resp.get() {
            Some(result) => {
                let response = result
                    .map_err(|()| "response error".to_string())?
                    .map_err(|e| format!("http error: {e:?}"))?;

                let status = response.status();
                let incoming_body = response.consume().map_err(|_| "consume response")?;
                let stream = incoming_body.stream().map_err(|_| "response body stream")?;

                let mut resp_bytes = Vec::new();
                loop {
                    match stream.blocking_read(8192) {
                        Ok(chunk) if chunk.is_empty() => break,
                        Ok(chunk) => resp_bytes.extend_from_slice(&chunk),
                        Err(StreamError::Closed) => break,
                        Err(StreamError::LastOperationFailed(_)) => break,
                    }
                }

                return Ok((status, resp_bytes));
            }
            None => {
                future_resp.subscribe().block();
            }
        }
    }
}

// ── Blobstore helpers ────────────────────────────────────────────────────────

fn doc_prefix(source: &proto::DocSource) -> String {
    let st = &source.source_type;
    let owner = &source.owner;
    let repo = if source.repo.is_empty() {
        owner.as_str()
    } else {
        source.repo.as_str()
    };
    let ver = &source.ref_or_ver;
    format!("docs/{st}/{owner}/{repo}/{ver}")
}

fn write_manifest(prefix: &str, manifest: &Manifest) -> Result<(), ServiceError> {
    let json = serde_json::to_vec(manifest)
        .map_err(|e| ServiceError::internal(format!("serialize manifest: {e}")))?;
    let key = format!("{prefix}/manifest.json");
    store::put_object(BUCKET, &key, &json)
        .map_err(|e| ServiceError::internal(format!("put manifest: {e:?}")))?;
    Ok(())
}

fn store_file(key: &str, data: &[u8]) -> Result<(), ServiceError> {
    store::put_object(BUCKET, key, data)
        .map_err(|e| ServiceError::internal(format!("put_object({key}): {e:?}")))?;
    Ok(())
}

// ── GitHub tarball fetcher ───────────────────────────────────────────────────

fn fetch_github_tarball(
    source: &proto::DocSource,
    prefix: &str,
) -> Result<u64, ServiceError> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    use tar::Archive;

    let url = format!(
        "https://api.github.com/repos/{}/{}/tarball/{}",
        source.owner, source.repo, source.ref_or_ver
    );
    wr_sdk::log::log(&format!("fetching tarball: {url}"));

    let (status, body) = http_get(&url)
        .map_err(|e| ServiceError::internal(format!("http_get: {e}")))?;

    if status != 200 {
        return Err(ServiceError::internal(format!(
            "GitHub API returned {status}"
        )));
    }

    // Decompress gzip + extract tar entries.
    let decoder = GzDecoder::new(body.as_slice());
    let mut archive = Archive::new(decoder);
    let mut entries = Vec::new();
    let mut total_bytes: u64 = 0;

    let text_extensions = [
        "rs", "md", "toml", "json", "txt", "yaml", "yml", "proto", "wit", "sql", "sh",
        "py", "js", "ts", "go", "c", "h", "cpp", "hpp", "html", "css",
    ];

    for entry_result in archive.entries().map_err(|e| {
        ServiceError::internal(format!("tar entries: {e}"))
    })? {
        let mut entry = match entry_result {
            Ok(e) => e,
            Err(_) => continue,
        };

        let path = match entry.path() {
            Ok(p) => p.to_string_lossy().to_string(),
            Err(_) => continue,
        };

        // GitHub tarballs have a top-level directory (owner-repo-sha/).
        // Strip it to get the relative path.
        let relative = match path.find('/') {
            Some(i) => &path[i + 1..],
            None => continue,
        };

        if relative.is_empty() {
            continue;
        }

        // Only extract text files.
        let is_text = text_extensions
            .iter()
            .any(|ext| relative.ends_with(&format!(".{ext}")));
        if !is_text {
            continue;
        }

        let mut contents = Vec::new();
        if entry.read_to_end(&mut contents).is_err() {
            continue;
        }

        // Skip very large files (> 512 KB) to stay within memory budget.
        if contents.len() > 512 * 1024 {
            continue;
        }

        let key = format!("{prefix}/files/{relative}");
        store_file(&key, &contents)?;

        let size = contents.len() as u64;
        total_bytes += size;
        entries.push(ManifestEntry {
            key: key.clone(),
            size,
        });
    }

    write_manifest(
        prefix,
        &Manifest {
            source_type: "github_tarball".into(),
            owner: source.owner.clone(),
            repo: source.repo.clone(),
            ref_or_ver: source.ref_or_ver.clone(),
            files: entries,
            total_bytes,
        },
    )?;

    wr_sdk::log::log(&format!(
        "stored tarball: {prefix} ({total_bytes} bytes)"
    ));
    Ok(total_bytes)
}

// ── docs.rs fetcher ──────────────────────────────────────────────────────────

fn fetch_docs_rs(
    source: &proto::DocSource,
    prefix: &str,
) -> Result<u64, ServiceError> {
    // Fetch the main crate doc page.
    let crate_name = &source.owner;
    let version = &source.ref_or_ver;
    let url = format!("https://docs.rs/{crate_name}/{version}/{crate_name}/index.html");
    wr_sdk::log::log(&format!("fetching docs.rs: {url}"));

    let (status, body) = http_get(&url)
        .map_err(|e| ServiceError::internal(format!("http_get: {e}")))?;

    if status != 200 {
        return Err(ServiceError::internal(format!(
            "docs.rs returned {status} for {url}"
        )));
    }

    let key = format!("{prefix}/files/index.html");
    let size = body.len() as u64;
    store_file(&key, &body)?;

    write_manifest(
        prefix,
        &Manifest {
            source_type: "docs_rs".into(),
            owner: crate_name.clone(),
            repo: String::new(),
            ref_or_ver: version.clone(),
            files: vec![ManifestEntry {
                key: key.clone(),
                size,
            }],
            total_bytes: size,
        },
    )?;

    wr_sdk::log::log(&format!("stored docs.rs: {prefix} ({size} bytes)"));
    Ok(size)
}

// ── crates.io fetcher ────────────────────────────────────────────────────────

fn fetch_crates_io(
    source: &proto::DocSource,
    prefix: &str,
) -> Result<u64, ServiceError> {
    let crate_name = &source.owner;
    let version = &source.ref_or_ver;
    let url = format!("https://crates.io/api/v1/crates/{crate_name}/{version}");
    wr_sdk::log::log(&format!("fetching crates.io: {url}"));

    let (status, body) = http_get(&url)
        .map_err(|e| ServiceError::internal(format!("http_get: {e}")))?;

    if status != 200 {
        return Err(ServiceError::internal(format!(
            "crates.io returned {status} for {url}"
        )));
    }

    let key = format!("{prefix}/files/metadata.json");
    let size = body.len() as u64;
    store_file(&key, &body)?;

    write_manifest(
        prefix,
        &Manifest {
            source_type: "crates_io".into(),
            owner: crate_name.clone(),
            repo: String::new(),
            ref_or_ver: version.clone(),
            files: vec![ManifestEntry {
                key: key.clone(),
                size,
            }],
            total_bytes: size,
        },
    )?;

    wr_sdk::log::log(&format!("stored crates.io: {prefix} ({size} bytes)"));
    Ok(size)
}
