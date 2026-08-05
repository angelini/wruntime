/// Write a message to WASI stderr, followed by a newline.
pub fn log(msg: &str) {
    use crate::bindings::wasi::cli::stderr;
    let err = stderr::get_stderr();
    for chunk in msg.as_bytes().chunks(4096) {
        let _ = err.blocking_write_and_flush(chunk);
    }
    let _ = err.blocking_write_and_flush(b"\n");
}

#[doc(hidden)]
pub fn log_format(args: std::fmt::Arguments<'_>) {
    #[cfg(not(test))]
    log(&args.to_string());
    #[cfg(test)]
    TEST_MESSAGES.with(|messages| messages.borrow_mut().push(args.to_string()));
}

#[cfg(test)]
thread_local! {
    static TEST_MESSAGES: std::cell::RefCell<Vec<String>> = const {
        std::cell::RefCell::new(Vec::new())
    };
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    #[test]
    fn formatting_macro_delegates_once_and_evaluates_arguments_once() {
        super::TEST_MESSAGES.with(|messages| messages.borrow_mut().clear());
        crate::log!("plain message");
        let evaluations = Cell::new(0_u32);
        crate::log!("captured value: {}", {
            evaluations.set(evaluations.get() + 1);
            42
        });
        assert_eq!(evaluations.get(), 1);
        super::TEST_MESSAGES.with(|messages| {
            assert_eq!(
                messages.borrow().as_slice(),
                ["plain message", "captured value: 42"]
            );
        });
    }
}
