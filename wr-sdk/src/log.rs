/// Write a message to WASI stderr, followed by a newline.
pub fn log(msg: &str) {
    use crate::bindings::wasi::cli::stderr;
    let err = stderr::get_stderr();
    let _ = err.blocking_write_and_flush(msg.as_bytes());
    let _ = err.blocking_write_and_flush(b"\n");
}
