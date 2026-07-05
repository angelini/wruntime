use crate::http::{HttpError, HttpRequest, Method};

/// Submit a job to a worker module's engine-managed queue.
///
/// `engine_authority` is the worker's `namespace.name` (e.g. `"codegen.worker"`).
/// Returns the job_id on success.
pub fn submit_job(
    engine_authority: &str,
    job_type: &str,
    payload: &[u8],
) -> Result<String, HttpError> {
    submit_job_with_options(engine_authority, job_type, payload, 0, 0)
}

/// Submit a job with explicit stale-running timeout and retry settings.
///
/// `timeout_secs` controls stale-running recovery in the queue; worker dispatch
/// uses the worker pool's configured job timeout. Pass 0 for `timeout_secs` or
/// `max_attempts` to use the engine queue defaults.
pub fn submit_job_with_options(
    engine_authority: &str,
    job_type: &str,
    payload: &[u8],
    timeout_secs: i32,
    max_attempts: i32,
) -> Result<String, HttpError> {
    // Parse namespace.name from the authority.
    let (namespace, name) = engine_authority.split_once('.').ok_or_else(|| {
        HttpError::Transport(format!(
            "invalid authority: {engine_authority} (expected namespace.name)"
        ))
    })?;

    let body = encode_submit_job_request(
        namespace,
        name,
        job_type,
        payload,
        timeout_secs,
        max_attempts,
    );
    let resp = crate::http::http_request(&HttpRequest {
        authority: engine_authority,
        path: "/SubmitJob",
        method: Method::Post,
        headers: &[("content-type", b"application/x-protobuf" as &[u8])],
        body: &body,
    })?;

    if resp.status != 200 {
        return Err(HttpError::Status {
            code: resp.status,
            body: resp.body,
        });
    }

    // Decode SubmitJobResponse: field 1 (string) = job_id
    decode_string_field(&resp.body, 1)
        .ok_or_else(|| HttpError::Decode("missing job_id in response".into()))
}

/// Query the status of a previously submitted job.
pub fn get_job_status(engine_authority: &str, job_id: &str) -> Result<JobStatus, HttpError> {
    let body = encode_string_field(1, job_id);
    let resp = crate::http::http_request(&HttpRequest {
        authority: engine_authority,
        path: "/wruntime.WorkerService/GetJobStatus",
        method: Method::Post,
        headers: &[("content-type", b"application/x-protobuf" as &[u8])],
        body: &body,
    })?;

    if resp.status != 200 {
        return Err(HttpError::Status {
            code: resp.status,
            body: resp.body,
        });
    }

    Ok(decode_job_status_response(&resp.body))
}

/// Status of a worker job.
pub struct JobStatus {
    pub job_id: String,
    pub status: String,
    pub result: Vec<u8>,
    pub error_message: String,
    pub attempt: i32,
    pub max_attempts: i32,
}

// ── Minimal protobuf encoding/decoding (no prost dependency) ────────────────

fn encode_varint(mut val: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (val & 0x7F) as u8;
        val >>= 7;
        if val == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn encode_string_field(field: u32, s: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    if !s.is_empty() {
        encode_varint(((field as u64) << 3) | 2, &mut buf); // wire type 2 = length-delimited
        encode_varint(s.len() as u64, &mut buf);
        buf.extend_from_slice(s.as_bytes());
    }
    buf
}

fn encode_bytes_field(field: u32, data: &[u8], buf: &mut Vec<u8>) {
    if !data.is_empty() {
        encode_varint(((field as u64) << 3) | 2, buf);
        encode_varint(data.len() as u64, buf);
        buf.extend_from_slice(data);
    }
}

fn encode_int32_field(field: u32, val: i32, buf: &mut Vec<u8>) {
    if val != 0 {
        encode_varint((field as u64) << 3, buf); // wire type 0 = varint
        encode_varint(val as u32 as u64, buf);
    }
}

fn encode_submit_job_request(
    namespace: &str,
    name: &str,
    job_type: &str,
    payload: &[u8],
    timeout_secs: i32,
    max_attempts: i32,
) -> Vec<u8> {
    let mut buf = Vec::new();
    // field 1: worker_namespace
    buf.extend_from_slice(&encode_string_field(1, namespace));
    // field 2: worker_name
    buf.extend_from_slice(&encode_string_field(2, name));
    // field 3: worker_version (empty)
    // field 4: job_type
    buf.extend_from_slice(&encode_string_field(4, job_type));
    // field 5: payload
    encode_bytes_field(5, payload, &mut buf);
    // field 6: timeout_secs
    encode_int32_field(6, timeout_secs, &mut buf);
    // field 7: max_attempts
    encode_int32_field(7, max_attempts, &mut buf);
    buf
}

fn decode_varint(data: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    while *pos < data.len() {
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

fn decode_string_field(data: &[u8], target_field: u32) -> Option<String> {
    let mut pos = 0;
    while pos < data.len() {
        let tag = decode_varint(data, &mut pos)?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        match wire_type {
            0 => {
                // varint — skip
                decode_varint(data, &mut pos)?;
            }
            2 => {
                // length-delimited
                let len = decode_varint(data, &mut pos)? as usize;
                if field_num == target_field {
                    let s = core::str::from_utf8(&data[pos..pos + len]).ok()?;
                    return Some(s.to_string());
                }
                pos += len;
            }
            _ => return None, // unsupported wire type
        }
    }
    None
}

fn decode_bytes_field(data: &[u8], target_field: u32) -> Vec<u8> {
    let mut pos = 0;
    while pos < data.len() {
        let tag = match decode_varint(data, &mut pos) {
            Some(t) => t,
            None => return Vec::new(),
        };
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        match wire_type {
            0 => {
                decode_varint(data, &mut pos);
            }
            2 => {
                let len = match decode_varint(data, &mut pos) {
                    Some(l) => l as usize,
                    None => return Vec::new(),
                };
                if field_num == target_field {
                    return data[pos..pos + len].to_vec();
                }
                pos += len;
            }
            _ => return Vec::new(),
        }
    }
    Vec::new()
}

fn decode_int32_field(data: &[u8], target_field: u32) -> i32 {
    let mut pos = 0;
    while pos < data.len() {
        let tag = match decode_varint(data, &mut pos) {
            Some(t) => t,
            None => return 0,
        };
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        match wire_type {
            0 => {
                let val = match decode_varint(data, &mut pos) {
                    Some(v) => v,
                    None => return 0,
                };
                if field_num == target_field {
                    return val as i32;
                }
            }
            2 => {
                let len = match decode_varint(data, &mut pos) {
                    Some(l) => l as usize,
                    None => return 0,
                };
                pos += len;
            }
            _ => return 0,
        }
    }
    0
}

fn decode_job_status_response(data: &[u8]) -> JobStatus {
    JobStatus {
        job_id: decode_string_field(data, 1).unwrap_or_default(),
        status: decode_string_field(data, 2).unwrap_or_default(),
        result: decode_bytes_field(data, 3),
        error_message: decode_string_field(data, 4).unwrap_or_default(),
        attempt: decode_int32_field(data, 5),
        max_attempts: decode_int32_field(data, 6),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_varint_single_byte() {
        let mut buf = Vec::new();
        encode_varint(0, &mut buf);
        assert_eq!(buf, vec![0]);

        buf.clear();
        encode_varint(1, &mut buf);
        assert_eq!(buf, vec![1]);

        buf.clear();
        encode_varint(127, &mut buf);
        assert_eq!(buf, vec![127]);
    }

    #[test]
    fn test_encode_varint_multi_byte() {
        let mut buf = Vec::new();
        encode_varint(128, &mut buf);
        assert_eq!(buf, vec![0x80, 0x01]);

        buf.clear();
        encode_varint(300, &mut buf);
        assert_eq!(buf, vec![0xAC, 0x02]);
    }

    #[test]
    fn test_decode_varint_roundtrip() {
        for val in [0u64, 1, 127, 128, 300, 16384, u32::MAX as u64] {
            let mut buf = Vec::new();
            encode_varint(val, &mut buf);
            let mut pos = 0;
            let decoded = decode_varint(&buf, &mut pos).unwrap();
            assert_eq!(decoded, val, "failed for {val}");
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn test_encode_string_field_empty() {
        let buf = encode_string_field(1, "");
        assert!(buf.is_empty(), "empty strings should produce no output");
    }

    #[test]
    fn test_encode_decode_string_field() {
        let buf = encode_string_field(1, "hello");
        let decoded = decode_string_field(&buf, 1);
        assert_eq!(decoded, Some("hello".to_string()));
    }

    #[test]
    fn test_decode_string_field_wrong_field() {
        let buf = encode_string_field(2, "hello");
        let decoded = decode_string_field(&buf, 1);
        assert_eq!(decoded, None);
    }

    #[test]
    fn test_encode_decode_bytes_field() {
        let mut buf = Vec::new();
        encode_bytes_field(3, b"binary\x00data", &mut buf);
        let decoded = decode_bytes_field(&buf, 3);
        assert_eq!(decoded, b"binary\x00data");
    }

    #[test]
    fn test_encode_decode_int32_field() {
        let mut buf = Vec::new();
        encode_int32_field(5, 42, &mut buf);
        let decoded = decode_int32_field(&buf, 5);
        assert_eq!(decoded, 42);
    }

    #[test]
    fn test_int32_field_zero_not_encoded() {
        let mut buf = Vec::new();
        encode_int32_field(5, 0, &mut buf);
        assert!(buf.is_empty(), "zero int32 should not be encoded");
        let decoded = decode_int32_field(&buf, 5);
        assert_eq!(decoded, 0);
    }

    #[test]
    fn test_submit_job_request_encoding() {
        let buf = encode_submit_job_request("my-ns", "my-mod", "/test/Rpc", b"payload", 60, 3);

        // Decode individual fields.
        assert_eq!(
            decode_string_field(&buf, 1),
            Some("my-ns".to_string()),
            "namespace"
        );
        assert_eq!(
            decode_string_field(&buf, 2),
            Some("my-mod".to_string()),
            "name"
        );
        assert_eq!(
            decode_string_field(&buf, 4),
            Some("/test/Rpc".to_string()),
            "job_type"
        );
        assert_eq!(decode_bytes_field(&buf, 5), b"payload", "payload");
        assert_eq!(decode_int32_field(&buf, 6), 60, "timeout_secs");
        assert_eq!(decode_int32_field(&buf, 7), 3, "max_attempts");
    }

    #[test]
    fn test_decode_job_status_response_all_fields() {
        // Manually build a protobuf with all 6 fields.
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_string_field(1, "job-123"));
        buf.extend_from_slice(&encode_string_field(2, "complete"));
        encode_bytes_field(3, b"result-bytes", &mut buf);
        buf.extend_from_slice(&encode_string_field(4, ""));
        encode_int32_field(5, 2, &mut buf);
        encode_int32_field(6, 3, &mut buf);

        let status = decode_job_status_response(&buf);
        assert_eq!(status.job_id, "job-123");
        assert_eq!(status.status, "complete");
        assert_eq!(status.result, b"result-bytes");
        assert_eq!(status.error_message, "");
        assert_eq!(status.attempt, 2);
        assert_eq!(status.max_attempts, 3);
    }

    #[test]
    fn test_decode_job_status_response_empty() {
        let status = decode_job_status_response(&[]);
        assert_eq!(status.job_id, "");
        assert_eq!(status.status, "");
        assert!(status.result.is_empty());
        assert_eq!(status.attempt, 0);
        assert_eq!(status.max_attempts, 0);
    }

    #[test]
    fn test_multi_field_message_decode() {
        // Encode two string fields and one int, then decode each.
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_string_field(1, "first"));
        buf.extend_from_slice(&encode_string_field(2, "second"));
        encode_int32_field(3, 99, &mut buf);

        assert_eq!(decode_string_field(&buf, 1), Some("first".to_string()));
        assert_eq!(decode_string_field(&buf, 2), Some("second".to_string()));
        assert_eq!(decode_int32_field(&buf, 3), 99);
        // Non-existent field.
        assert_eq!(decode_string_field(&buf, 4), None);
        assert_eq!(decode_int32_field(&buf, 4), 0);
    }
}
