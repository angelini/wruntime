use crate::http::{HttpError, HttpRequest, Method};

const SUBMIT_JOB_PATH: &str = "/wruntime.WorkerService/SubmitJob";
const GET_JOB_STATUS_PATH: &str = "/wruntime.WorkerService/GetJobStatus";

/// Submit a job to a worker module's engine-managed queue.
///
/// `engine_authority` is the worker's `namespace.name` (e.g. `"codegen.worker"`).
/// `worker_version` must be non-empty.
/// Returns the job_id on success.
pub fn submit_job(
    engine_authority: &str,
    worker_version: &str,
    job_type: &str,
    payload: &[u8],
) -> Result<String, HttpError> {
    submit_job_with_options(engine_authority, worker_version, job_type, payload, 0, 0)
}

fn submit_job_headers(worker_version: &str) -> [(&str, &[u8]); 2] {
    [
        ("content-type", b"application/x-protobuf" as &[u8]),
        ("x-wr-version", worker_version.as_bytes()),
    ]
}

/// Submit a job with explicit stale-running timeout and retry settings.
///
/// `engine_authority` is the worker's `namespace.name` (e.g. `"codegen.worker"`).
/// `worker_version` must be non-empty. `timeout_secs` controls stale-running
/// recovery in the queue; worker dispatch uses the worker pool's configured job
/// timeout. Pass 0 for `timeout_secs`; `max_attempts = 0` uses
/// engine-configured worker defaults.
pub fn submit_job_with_options(
    engine_authority: &str,
    worker_version: &str,
    job_type: &str,
    payload: &[u8],
    timeout_secs: i32,
    max_attempts: i32,
) -> Result<String, HttpError> {
    if worker_version.is_empty() {
        return Err(HttpError::Transport("worker_version is required".into()));
    }

    // Parse namespace.name from the authority.
    let (namespace, name) = engine_authority.split_once('.').ok_or_else(|| {
        HttpError::Transport(format!(
            "invalid authority: {engine_authority} (expected namespace.name)"
        ))
    })?;

    let body = encode_submit_job_request(
        namespace,
        name,
        worker_version,
        job_type,
        payload,
        timeout_secs,
        max_attempts,
    );
    let headers = submit_job_headers(worker_version);
    let resp = crate::http::http_request(&HttpRequest {
        authority: engine_authority,
        path: SUBMIT_JOB_PATH,
        method: Method::Post,
        headers: &headers,
        body: &body,
    })?;

    if resp.status != 200 {
        return Err(HttpError::Status {
            code: resp.status,
            body: resp.body,
        });
    }

    decode_submit_job_response(&resp.body)
}

/// Query the status of a previously submitted job.
pub fn get_job_status(engine_authority: &str, job_id: &str) -> Result<JobStatus, HttpError> {
    let body = encode_string_field(1, job_id);
    let resp = crate::http::http_request(&HttpRequest {
        authority: engine_authority,
        path: GET_JOB_STATUS_PATH,
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

    decode_job_status_response(&resp.body)
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
    worker_version: &str,
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
    // field 3: worker_version
    buf.extend_from_slice(&encode_string_field(3, worker_version));
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

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, HttpError> {
    let mut result: u64 = 0;
    let mut shift = 0;

    for i in 0..10 {
        if *pos >= data.len() {
            return Err(HttpError::Decode("truncated varint".into()));
        }
        let byte = data[*pos];
        *pos += 1;

        if i == 9 && byte > 1 {
            return Err(HttpError::Decode("varint overflow".into()));
        }

        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }

    Err(HttpError::Decode("varint overflow".into()))
}

fn read_len(data: &[u8], pos: &mut usize) -> Result<usize, HttpError> {
    let len = decode_varint(data, pos)?;
    usize::try_from(len).map_err(|_| HttpError::Decode("length does not fit usize".into()))
}

fn checked_slice_end(pos: usize, len: usize, data_len: usize) -> Result<usize, HttpError> {
    let end = pos
        .checked_add(len)
        .ok_or_else(|| HttpError::Decode("length overflow".into()))?;
    if end > data_len {
        return Err(HttpError::Decode("truncated length-delimited field".into()));
    }
    Ok(end)
}

fn skip_field(data: &[u8], pos: &mut usize, wire_type: u8) -> Result<(), HttpError> {
    match wire_type {
        0 => {
            let _ = decode_varint(data, pos)?;
            Ok(())
        }
        2 => {
            let len = read_len(data, pos)?;
            let end = checked_slice_end(*pos, len, data.len())?;
            *pos = end;
            Ok(())
        }
        _ => Err(HttpError::Decode(format!(
            "unsupported wire type {wire_type}"
        ))),
    }
}

fn decode_string_field(data: &[u8], target_field: u32) -> Result<Option<String>, HttpError> {
    let mut pos = 0;
    while pos < data.len() {
        let tag = decode_varint(data, &mut pos)?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        if field_num == 0 {
            return Err(HttpError::Decode("invalid field number 0".into()));
        }

        if field_num == target_field {
            if wire_type != 2 {
                return Err(HttpError::Decode(format!(
                    "field {target_field} has wire type {wire_type}, expected 2"
                )));
            }
            let len = read_len(data, &mut pos)?;
            let end = checked_slice_end(pos, len, data.len())?;
            let s = core::str::from_utf8(&data[pos..end])
                .map_err(|e| HttpError::Decode(format!("invalid UTF-8: {e}")))?;
            return Ok(Some(s.to_string()));
        }

        skip_field(data, &mut pos, wire_type)?;
    }
    Ok(None)
}

fn decode_bytes_field(data: &[u8], target_field: u32) -> Result<Vec<u8>, HttpError> {
    let mut pos = 0;
    while pos < data.len() {
        let tag = decode_varint(data, &mut pos)?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        if field_num == 0 {
            return Err(HttpError::Decode("invalid field number 0".into()));
        }

        if field_num == target_field {
            if wire_type != 2 {
                return Err(HttpError::Decode(format!(
                    "field {target_field} has wire type {wire_type}, expected 2"
                )));
            }
            let len = read_len(data, &mut pos)?;
            let end = checked_slice_end(pos, len, data.len())?;
            return Ok(data[pos..end].to_vec());
        }

        skip_field(data, &mut pos, wire_type)?;
    }
    Ok(Vec::new())
}

fn decode_int32_field(data: &[u8], target_field: u32) -> Result<i32, HttpError> {
    let mut pos = 0;
    while pos < data.len() {
        let tag = decode_varint(data, &mut pos)?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        if field_num == 0 {
            return Err(HttpError::Decode("invalid field number 0".into()));
        }

        if field_num == target_field {
            if wire_type != 0 {
                return Err(HttpError::Decode(format!(
                    "field {target_field} has wire type {wire_type}, expected 0"
                )));
            }
            return Ok(decode_varint(data, &mut pos)? as i32);
        }

        skip_field(data, &mut pos, wire_type)?;
    }
    Ok(0)
}

fn decode_submit_job_response(data: &[u8]) -> Result<String, HttpError> {
    decode_string_field(data, 1)?
        .filter(|job_id| !job_id.is_empty())
        .ok_or_else(|| HttpError::Decode("missing job_id in response".into()))
}

fn decode_job_status_response(data: &[u8]) -> Result<JobStatus, HttpError> {
    let job_id = decode_string_field(data, 1)?
        .filter(|job_id| !job_id.is_empty())
        .ok_or_else(|| HttpError::Decode("missing job_id in response".into()))?;
    let status = decode_string_field(data, 2)?
        .filter(|status| !status.is_empty())
        .ok_or_else(|| HttpError::Decode("missing status in response".into()))?;

    Ok(JobStatus {
        job_id,
        status,
        result: decode_bytes_field(data, 3)?,
        error_message: decode_string_field(data, 4)?.unwrap_or_default(),
        attempt: decode_int32_field(data, 5)?,
        max_attempts: decode_int32_field(data, 6)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_management_paths_are_canonical() {
        assert_eq!(SUBMIT_JOB_PATH, "/wruntime.WorkerService/SubmitJob");
        assert_eq!(GET_JOB_STATUS_PATH, "/wruntime.WorkerService/GetJobStatus");
    }

    #[test]
    fn test_submit_job_rejects_empty_worker_version_locally() {
        let err = submit_job_with_options("my-ns.my-mod", "", "/test/Rpc", b"payload", 0, 0)
            .expect_err("empty worker_version must be rejected before HTTP");
        assert!(matches!(err, HttpError::Transport(msg) if msg == "worker_version is required"));
    }

    #[test]
    fn test_submit_job_headers_pin_worker_version() {
        let headers = submit_job_headers("1.2.3");
        assert_eq!(
            headers[0],
            ("content-type", b"application/x-protobuf" as &[u8])
        );
        assert_eq!(headers[1], ("x-wr-version", b"1.2.3" as &[u8]));
    }

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
        let decoded = decode_string_field(&buf, 1).unwrap();
        assert_eq!(decoded, Some("hello".to_string()));
    }

    #[test]
    fn test_decode_string_field_wrong_field() {
        let buf = encode_string_field(2, "hello");
        let decoded = decode_string_field(&buf, 1).unwrap();
        assert_eq!(decoded, None);
    }

    #[test]
    fn test_encode_decode_bytes_field() {
        let mut buf = Vec::new();
        encode_bytes_field(3, b"binary\x00data", &mut buf);
        let decoded = decode_bytes_field(&buf, 3).unwrap();
        assert_eq!(decoded, b"binary\x00data");
    }

    #[test]
    fn test_encode_decode_int32_field() {
        let mut buf = Vec::new();
        encode_int32_field(5, 42, &mut buf);
        let decoded = decode_int32_field(&buf, 5).unwrap();
        assert_eq!(decoded, 42);
    }

    #[test]
    fn test_int32_field_zero_not_encoded() {
        let mut buf = Vec::new();
        encode_int32_field(5, 0, &mut buf);
        assert!(buf.is_empty(), "zero int32 should not be encoded");
        let decoded = decode_int32_field(&buf, 5).unwrap();
        assert_eq!(decoded, 0);
    }

    #[test]
    fn test_submit_job_request_encoding() {
        let buf =
            encode_submit_job_request("my-ns", "my-mod", "1.2.3", "/test/Rpc", b"payload", 60, 3);

        // Decode individual fields.
        assert_eq!(
            decode_string_field(&buf, 1).unwrap(),
            Some("my-ns".to_string()),
            "namespace"
        );
        assert_eq!(
            decode_string_field(&buf, 2).unwrap(),
            Some("my-mod".to_string()),
            "name"
        );
        assert_eq!(
            decode_string_field(&buf, 3).unwrap(),
            Some("1.2.3".to_string()),
            "worker_version"
        );
        assert_eq!(
            decode_string_field(&buf, 4).unwrap(),
            Some("/test/Rpc".to_string()),
            "job_type"
        );
        assert_eq!(decode_bytes_field(&buf, 5).unwrap(), b"payload", "payload");
        assert_eq!(decode_int32_field(&buf, 6).unwrap(), 60, "timeout_secs");
        assert_eq!(decode_int32_field(&buf, 7).unwrap(), 3, "max_attempts");

        let override_buf =
            encode_submit_job_request("my-ns", "my-mod", "1.2.3", "/test/Rpc", b"payload", 0, 9);
        assert_eq!(
            decode_int32_field(&override_buf, 6).unwrap(),
            0,
            "timeout_secs"
        );
        assert_eq!(
            decode_int32_field(&override_buf, 7).unwrap(),
            9,
            "max_attempts"
        );
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

        let status = decode_job_status_response(&buf).unwrap();
        assert_eq!(status.job_id, "job-123");
        assert_eq!(status.status, "complete");
        assert_eq!(status.result, b"result-bytes");
        assert_eq!(status.error_message, "");
        assert_eq!(status.attempt, 2);
        assert_eq!(status.max_attempts, 3);
    }

    #[test]
    fn test_decode_job_status_response_empty() {
        assert!(matches!(
            decode_job_status_response(&[]),
            Err(HttpError::Decode(_))
        ));
    }

    #[test]
    fn test_decode_submit_job_response_missing_job_id_errors() {
        assert!(matches!(
            decode_submit_job_response(&[]),
            Err(HttpError::Decode(_))
        ));
    }

    #[test]
    fn test_decode_job_status_defaults_absent_detail_fields() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_string_field(1, "job-123"));
        buf.extend_from_slice(&encode_string_field(2, "pending"));

        let status = decode_job_status_response(&buf).unwrap();
        assert_eq!(status.job_id, "job-123");
        assert_eq!(status.status, "pending");
        assert!(status.result.is_empty());
        assert_eq!(status.error_message, "");
        assert_eq!(status.attempt, 0);
        assert_eq!(status.max_attempts, 0);
    }

    #[test]
    fn test_decode_rejects_truncated_length_delimited_field() {
        let data = [0x0a, 0x05, b'a'];
        assert!(matches!(
            decode_string_field(&data, 1),
            Err(HttpError::Decode(_))
        ));
    }

    #[test]
    fn test_decode_rejects_unsupported_wire_type() {
        let data = [0x0b];
        assert!(matches!(
            decode_string_field(&data, 1),
            Err(HttpError::Decode(_))
        ));
    }

    #[test]
    fn test_decode_rejects_overflowing_varint() {
        let data = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
        let mut pos = 0;
        assert!(matches!(
            decode_varint(&data, &mut pos),
            Err(HttpError::Decode(_))
        ));
    }

    #[test]
    fn test_decode_rejects_invalid_utf8() {
        let data = [0x0a, 0x01, 0xff];
        assert!(matches!(
            decode_string_field(&data, 1),
            Err(HttpError::Decode(_))
        ));
    }

    #[test]
    fn test_multi_field_message_decode() {
        // Encode two string fields and one int, then decode each.
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_string_field(1, "first"));
        buf.extend_from_slice(&encode_string_field(2, "second"));
        encode_int32_field(3, 99, &mut buf);

        assert_eq!(
            decode_string_field(&buf, 1).unwrap(),
            Some("first".to_string())
        );
        assert_eq!(
            decode_string_field(&buf, 2).unwrap(),
            Some("second".to_string())
        );
        assert_eq!(decode_int32_field(&buf, 3).unwrap(), 99);
        // Non-existent field.
        assert_eq!(decode_string_field(&buf, 4).unwrap(), None);
        assert_eq!(decode_int32_field(&buf, 4).unwrap(), 0);
    }
}
