//! Integration-test placeholder.
//!
//! `stryke-search` is a `cdylib`-only crate (no `rlib`), so an integration
//! test in `tests/` cannot link against its `extern "C"` exports — there is
//! no Rust-visible API surface to call from here. The real coverage is:
//!
//!   * `src/lib.rs` `#[cfg(test)] mod tests` — unit tests for every piece of
//!     pure logic (NDJSON build, Lucene escaping, URL parse/redact, basic-auth
//!     header, percent-encoding). These run on `cargo test`.
//!   * `t/test_stryke_search_surface.stk` — pins that every `Search::*`
//!     wrapper resolves and that the query-DSL builders produce the right
//!     shape, with no cluster required.
//!   * `t/test_search.stk` — end-to-end CRUD + search against a live
//!     Elasticsearch/OpenSearch at `$SEARCH_URL`, short-circuited when no
//!     cluster answers.
//!
//! This file keeps the `tests/` directory populated and gives `cargo test` a
//! green integration target.

#[test]
fn cdylib_crate_compiles() {
    // Reaching this test means the crate (and thus every `extern "C"`
    // `search__*` export) type-checked and linked into the test harness's
    // dependency graph. That is the minimum contract an integration test can
    // assert about a cdylib-only crate.
}
