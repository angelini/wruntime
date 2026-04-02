/// Write a message to WASI stderr, followed by a newline.
pub fn log(msg: &str) {
    use crate::bindings::wasi::cli::stderr;
    let err = stderr::get_stderr();
    for chunk in msg.as_bytes().chunks(4096) {
        let _ = err.blocking_write_and_flush(chunk);
    }
    let _ = err.blocking_write_and_flush(b"\n");
}
