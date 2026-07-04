//! Compile tests that pin the `Send` contract of public `async` API futures.

#[test]
fn auto_trait_trybuild() {
    let t = trybuild::TestCases::new();
    t.pass("tests/auto_trait_compile_pass/*.rs");
    t.compile_fail("tests/auto_trait_compile_fail/*.rs");
}
