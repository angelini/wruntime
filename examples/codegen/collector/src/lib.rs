#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "collector",
        generate_all,
    });
}

use serde::{Deserialize, Serialize};
use wr_sdk::bindings::wruntime::blobstore::store;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

const BUCKET: &str = "codegen";

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::collector_service_handle(&Component, request, response_out);
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
        let span = wr_sdk::span!("collector.fetch_docs", "sources.count" => req.sources.len());

        let mut sources_fetched: u32 = 0;
        let mut total_bytes: u64 = 0;
        let mut doc_prefixes = Vec::new();

        for source in &req.sources {
            // For github_tarball sources with an empty ref, resolve the default
            // branch up front so the blobstore prefix includes the real ref.
            let mut source = source.clone();
            let source_type = proto::DocSourceType::try_from(source.source_type)
                .map_err(|_| ServiceError::bad_request("unknown source_type"))?;
            if source_type == proto::DocSourceType::Unspecified {
                return Err(ServiceError::bad_request("source_type is required"));
            }
            if source_type == proto::DocSourceType::GithubTarball && source.ref_or_ver.is_empty() {
                source.ref_or_ver = resolve_github_ref(&source.owner, &source.repo)?;
            }

            let prefix = doc_prefix(&source);
            let manifest_key = format!("{prefix}/manifest.json");

            // Check if already fetched (idempotent).
            if store::head_object(BUCKET, &manifest_key).is_ok() {
                wr_sdk::log::log(&format!("skipping already-fetched: {prefix}"));
                tracing::record_event(&span, "cache_hit", &[("prefix", prefix.as_str())]);
                doc_prefixes.push(prefix);
                continue;
            }

            let fetch_span = tracing::start(
                "collector.fetch_source",
                &[
                    ("source.type", doc_source_type_name(source_type)),
                    ("source.owner", source.owner.as_str()),
                    ("source.repo", source.repo.as_str()),
                ],
            );
            let bytes = match source_type {
                proto::DocSourceType::GithubTarball => fetch_github_tarball(&source, &prefix),
                proto::DocSourceType::DocsRs => fetch_docs_rs(&source, &prefix),
                proto::DocSourceType::CratesIo => fetch_crates_io(&source, &prefix),
                proto::DocSourceType::Unspecified => unreachable!(),
            }
            .inspect_err(|e| {
                tracing::set_error(&fetch_span, &e.message);
            })?;

            tracing::set_attr(&fetch_span, "source.bytes", bytes);
            drop(fetch_span);

            total_bytes = total_bytes.saturating_add(bytes as u64);
            sources_fetched += 1;
            doc_prefixes.push(prefix);
        }

        tracing::set_attr(&span, "sources.fetched", sources_fetched);
        tracing::set_attr(&span, "total_bytes", total_bytes);
        drop(span);

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
        let objects = store::list_objects(BUCKET, Some(&req.doc_prefix))?;

        let chunks = objects
            .into_iter()
            .map(|obj| proto::DocChunkMeta {
                key: obj.key,
                size: obj.size,
                label: String::new(),
            })
            .collect();

        Ok(proto::ListDocsResponse { chunks })
    }
}

// ── HTTP egress helper ───────────────────────────────────────────────────────

const MAX_REDIRECTS: u8 = 10;

fn http_get(url: &str) -> Result<(u16, Vec<u8>), String> {
    let mut current_url = url.to_string();

    for _ in 0..MAX_REDIRECTS {
        let (status, headers, body) = http_get_raw(&current_url)?;

        match status {
            301 | 302 | 303 | 307 | 308 => {
                let location =
                    headers.ok_or_else(|| format!("{status} redirect with no Location header"))?;
                // Handle relative redirects.
                current_url = if location.starts_with("http://") || location.starts_with("https://")
                {
                    location
                } else {
                    // Build absolute URL from current authority + relative path.
                    let (scheme_str, rest) =
                        if let Some(rest) = current_url.strip_prefix("https://") {
                            ("https://", rest)
                        } else if let Some(rest) = current_url.strip_prefix("http://") {
                            ("http://", rest)
                        } else {
                            ("http://", current_url.as_str())
                        };
                    let authority = match rest.find('/') {
                        Some(i) => &rest[..i],
                        None => rest,
                    };
                    format!("{scheme_str}{authority}{location}")
                };
                wr_sdk::log::log(&format!("following {status} redirect to: {current_url}"));
            }
            _ => return Ok((status, body)),
        }
    }

    Err(format!("too many redirects (max {MAX_REDIRECTS})"))
}

fn http_get_raw(url: &str) -> Result<(u16, Option<String>, Vec<u8>), String> {
    use wr_sdk::bindings::wasi::http::{
        outgoing_handler,
        types::{Fields, Method, OutgoingBody, OutgoingRequest, Scheme},
    };
    use wr_sdk::bindings::wasi::io::streams::StreamError;

    // Parse URL: scheme://authority/path
    let (scheme, rest) = if let Some(rest) = url.strip_prefix("https://") {
        (Scheme::Https, rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (Scheme::Http, rest)
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

    let future_resp = outgoing_handler::handle(req, None).map_err(|e| format!("handle: {e:?}"))?;

    loop {
        match future_resp.get() {
            Some(result) => {
                let response = result
                    .map_err(|()| "response error".to_string())?
                    .map_err(|e| format!("http error: {e:?}"))?;

                let status = response.status();

                // Extract Location header for redirects.
                let location = response
                    .headers()
                    .get("location")
                    .first()
                    .and_then(|v| String::from_utf8(v.clone()).ok());

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

                return Ok((status, location, resp_bytes));
            }
            None => {
                future_resp.subscribe().block();
            }
        }
    }
}

// ── Blobstore helpers ────────────────────────────────────────────────────────

fn doc_source_type_name(source_type: proto::DocSourceType) -> &'static str {
    match source_type {
        proto::DocSourceType::GithubTarball => "github_tarball",
        proto::DocSourceType::DocsRs => "docs_rs",
        proto::DocSourceType::CratesIo => "crates_io",
        proto::DocSourceType::Unspecified => "unspecified",
    }
}

fn doc_prefix(source: &proto::DocSource) -> String {
    let st = proto::DocSourceType::try_from(source.source_type)
        .map(doc_source_type_name)
        .unwrap_or("unspecified");
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
    store::put_object(BUCKET, &key, &json)?;
    Ok(())
}

fn store_file(key: &str, data: &[u8]) -> Result<(), ServiceError> {
    store::put_object(BUCKET, key, data)?;
    Ok(())
}

// ── GitHub tarball fetcher ───────────────────────────────────────────────────

/// Query the GitHub API to discover a repository's default branch.
fn resolve_github_ref(owner: &str, repo: &str) -> Result<String, ServiceError> {
    let url = format!("https://api.github.com/repos/{owner}/{repo}");
    wr_sdk::log::log(&format!("resolving default branch: {url}"));

    let (status, body) =
        http_get(&url).map_err(|e| ServiceError::internal(format!("GitHub API: {e}")))?;

    if status != 200 {
        return Err(ServiceError::internal(format!(
            "GitHub API returned {status} for {url}"
        )));
    }

    #[derive(Deserialize)]
    struct RepoInfo {
        default_branch: String,
    }

    let info: RepoInfo = serde_json::from_slice(&body)
        .map_err(|e| ServiceError::internal(format!("parse repo info: {e}")))?;

    wr_sdk::log::log(&format!("resolved default branch: {}", info.default_branch));
    Ok(info.default_branch)
}

fn fetch_github_tarball(source: &proto::DocSource, prefix: &str) -> Result<u64, ServiceError> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    use tar::Archive;

    let url = format!(
        "https://codeload.github.com/{}/{}/tar.gz/{}",
        source.owner, source.repo, source.ref_or_ver
    );
    wr_sdk::log::log(&format!("fetching tarball: {url}"));

    let (status, body) =
        http_get(&url).map_err(|e| ServiceError::internal(format!("http_get: {e}")))?;

    if status != 200 {
        return Err(ServiceError::internal(format!(
            "GitHub tarball download returned {status}"
        )));
    }

    // Decompress gzip + extract tar entries.
    let decoder = GzDecoder::new(body.as_slice());
    let mut archive = Archive::new(decoder);
    let mut entries = Vec::new();
    let mut total_bytes: u64 = 0;

    let text_extensions = [
        "rs", "md", "toml", "json", "txt", "yaml", "yml", "proto", "wit", "sql", "sh", "py", "js",
        "ts", "go", "c", "h", "cpp", "hpp", "html", "css",
    ];

    for entry_result in archive
        .entries()
        .map_err(|e| ServiceError::internal(format!("tar entries: {e}")))?
    {
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

    wr_sdk::log::log(&format!("stored tarball: {prefix} ({total_bytes} bytes)"));
    Ok(total_bytes)
}

// ── docs.rs fetcher ──────────────────────────────────────────────────────────

fn fetch_docs_rs(source: &proto::DocSource, prefix: &str) -> Result<u64, ServiceError> {
    // Fetch the main crate doc page.
    let crate_name = &source.owner;
    let version = &source.ref_or_ver;
    let url = format!("https://docs.rs/{crate_name}/{version}/{crate_name}/index.html");
    wr_sdk::log::log(&format!("fetching docs.rs: {url}"));

    let (status, body) =
        http_get(&url).map_err(|e| ServiceError::internal(format!("http_get: {e}")))?;

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

fn fetch_crates_io(source: &proto::DocSource, prefix: &str) -> Result<u64, ServiceError> {
    let crate_name = &source.owner;
    let version = &source.ref_or_ver;
    let url = format!("https://crates.io/api/v1/crates/{crate_name}/{version}");
    wr_sdk::log::log(&format!("fetching crates.io: {url}"));

    let (status, body) =
        http_get(&url).map_err(|e| ServiceError::internal(format!("http_get: {e}")))?;

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
