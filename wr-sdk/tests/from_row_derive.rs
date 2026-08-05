#[test]
fn from_row_derive_ui() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/from_row/pass-basic.rs");
    tests.pass("tests/ui/from_row/pass-canonical.rs");
    tests.pass("tests/ui/from_row/pass-flatten.rs");
    if cfg!(feature = "serde") {
        tests.pass("tests/ui/from_row/pass-json.rs");
    }
    tests.compile_fail("tests/ui/from_row/fail-*.rs");
}
