//! stryke-search — Elasticsearch / OpenSearch cdylib loaded in-process by
//! stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn search__*` is a JSON-string-in /
//! JSON-string-out wrapper. stryke's FFI bridge (`rust_ffi.rs::load_cdylib`)
//! resolves these symbols at first `use Search`, registers each one as a
//! stryke-callable function, and on each call passes a JSON-encoded args dict
//! and copies the returned JSON into a stryke string. The cdylib's
//! `stryke_free_cstring` export frees that allocation.
//!
//! Transport is the cluster's REST API over `ureq` (sync, rustls). Both
//! Elasticsearch and OpenSearch speak the same `_search` / `_bulk` / `_doc`
//! / `_cat` endpoints, so one client covers both. Persistent state: `AGENTS`
//! caches one `ureq::Agent` (HTTP keep-alive connection pool) per
//! `(base_url, auth)` tuple for the life of the stryke process, so repeated
//! `Search::*` ops reuse pooled sockets instead of reconnecting per call.
//!
//! Network handlers fail loud when the cluster is unreachable; the pure
//! query-DSL builders (`search__match_query`, `search__bool_query`,
//! `search__escape_query_string`, …) take no connection and run anywhere —
//! they are what the surface tests exercise in CI without a live cluster.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{json, Value};

// ── connection cache ────────────────────────────────────────────────────────

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct ConnKey {
    base: String,
    auth: String,
}

static AGENTS: OnceCell<Mutex<HashMap<ConnKey, Arc<ureq::Agent>>>> = OnceCell::new();

fn agents() -> &'static Mutex<HashMap<ConnKey, Arc<ureq::Agent>>> {
    AGENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Resolve the base URL + `Authorization` header from an opts dict. Accepts
/// either an explicit `url` (`https://host:9200`) or `host`/`port`/`tls`
/// parts (default `127.0.0.1:9200`, plaintext). Auth precedence: `api_key`
/// (sent as `ApiKey <key>`) over `username`+`password` (HTTP Basic).
fn conn_from_opts(opts: &Value) -> ConnKey {
    let base = if let Some(u) = opts.get("url").and_then(|v| v.as_str()) {
        u.trim_end_matches('/').to_string()
    } else {
        let host = opts
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("127.0.0.1");
        let port = opts.get("port").and_then(|v| v.as_i64()).unwrap_or(9200);
        let tls = opts.get("tls").and_then(|v| v.as_bool()).unwrap_or(false);
        let scheme = if tls { "https" } else { "http" };
        format!("{}://{}:{}", scheme, host, port)
    };
    let auth = if let Some(k) = opts.get("api_key").and_then(|v| v.as_str()) {
        format!("ApiKey {}", k)
    } else {
        let user = opts.get("username").and_then(|v| v.as_str()).unwrap_or("");
        let pass = opts.get("password").and_then(|v| v.as_str()).unwrap_or("");
        if user.is_empty() && pass.is_empty() {
            String::new()
        } else {
            format!("Basic {}", B64.encode(format!("{}:{}", user, pass)))
        }
    };
    ConnKey { base, auth }
}

/// Get or build the cached `Agent` for this opts dict.
fn agent_for(opts: &Value) -> (Arc<ureq::Agent>, ConnKey) {
    let key = conn_from_opts(opts);
    let mut map = agents().lock();
    let agent = map
        .entry(key.clone())
        .or_insert_with(|| {
            Arc::new(
                ureq::AgentBuilder::new()
                    .timeout_connect(Duration::from_secs(10))
                    .timeout(Duration::from_secs(60))
                    .build(),
            )
        })
        .clone();
    (agent, key)
}

// ── HTTP plumbing ───────────────────────────────────────────────────────────

/// Perform one request and return `(status, body)` without treating a non-2xx
/// status as a transport error — callers decide what a 404/409 means.
fn do_http(opts: &Value, method: &str, path: &str, body: Option<String>) -> Result<(u16, String)> {
    let (agent, key) = agent_for(opts);
    let url = format!("{}/{}", key.base, path.trim_start_matches('/'));
    let mut rq = agent.request(method, &url);
    if !key.auth.is_empty() {
        rq = rq.set("Authorization", &key.auth);
    }
    let resp = match body {
        Some(b) => rq.set("Content-Type", "application/json").send_string(&b),
        None => rq.call(),
    };
    match resp {
        Ok(r) => {
            let status = r.status();
            let text = r.into_string().map_err(|e| anyhow!("read body: {}", e))?;
            Ok((status, text))
        }
        Err(ureq::Error::Status(code, r)) => {
            let text = r.into_string().unwrap_or_default();
            Ok((code, text))
        }
        Err(ureq::Error::Transport(t)) => Err(anyhow!("{} {}: {}", method, url, t)),
    }
}

/// Request expecting a JSON body. Parses 2xx bodies as JSON (empty → null);
/// turns any other status into an error carrying the cluster's response.
fn req_json(opts: &Value, method: &str, path: &str, body: Option<String>) -> Result<Value> {
    let (status, text) = do_http(opts, method, path, body)?;
    if (200..300).contains(&status) {
        if text.trim().is_empty() {
            Ok(Value::Null)
        } else {
            serde_json::from_str(&text).map_err(|e| anyhow!("parse response: {}", e))
        }
    } else {
        Err(anyhow!("HTTP {}: {}", status, text))
    }
}

/// HEAD-style existence probe: 2xx → true, 404 → false, anything else errors.
fn req_exists(opts: &Value, path: &str) -> Result<bool> {
    let (status, text) = do_http(opts, "HEAD", path, None)?;
    match status {
        s if (200..300).contains(&s) => Ok(true),
        404 => Ok(false),
        s => Err(anyhow!("HTTP {}: {}", s, text)),
    }
}

// ── small extractors ────────────────────────────────────────────────────────

fn str_field<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v.get(k)
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow!("missing {}", k))
}

fn opt_str<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(|x| x.as_str())
}

/// Resolve the JSON body for a `_search` / `_count`. An explicit `body` field
/// is sent verbatim. Otherwise the `query` field is taken as either a full
/// search body (when it carries a top-level `query`/`aggs`/`size`/… key) or a
/// bare query clause (e.g. `{"match":{…}}` from a builder), which is wrapped
/// as `{"query": <clause>}`. So `Search::search(idx, Search::match(…))` and a
/// hand-built `{ size, query, aggs }` body both work.
fn search_body(v: &Value) -> Option<String> {
    if let Some(b) = v.get("body").filter(|b| !b.is_null()) {
        return Some(b.to_string());
    }
    let q = v.get("query").filter(|b| !b.is_null())?;
    const FULL_BODY_KEYS: &[&str] = &[
        "query",
        "aggs",
        "aggregations",
        "size",
        "from",
        "sort",
        "_source",
        "highlight",
        "track_total_hits",
        "search_after",
        "pit",
        "collapse",
        "suggest",
    ];
    let is_full = q
        .as_object()
        .is_some_and(|o| FULL_BODY_KEYS.iter().any(|k| o.contains_key(*k)));
    Some(if is_full {
        q.to_string()
    } else {
        json!({ "query": q }).to_string()
    })
}

/// Append `?a=b&c=d` to `path` from the opts dict's `params` object, if any.
fn with_params(path: String, opts: &Value) -> String {
    let Some(params) = opts.get("params").and_then(|p| p.as_object()) else {
        return path;
    };
    if params.is_empty() {
        return path;
    }
    let q: Vec<String> = params
        .iter()
        .map(|(k, v)| {
            let val = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            format!("{}={}", urlencode(k), urlencode(&val))
        })
        .collect();
    let sep = if path.contains('?') { '&' } else { '?' };
    format!("{}{}{}", path, sep, q.join("&"))
}

/// Minimal RFC-3986 query-component percent-encoding. Unreserved bytes pass
/// through; everything else is `%XX`.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-search handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib. stryke's FFI
/// bridge calls this immediately after copying the returned bytes into a
/// stryke string.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── version + liveness ──────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn search__ping(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let (status, _) = do_http(&v, "GET", "/", None)?;
        Ok(json!({"value": (200..300).contains(&status)}))
    })
}

#[no_mangle]
pub extern "C" fn search__info(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| req_json(&v, "GET", "/", None))
}

#[no_mangle]
pub extern "C" fn search__health(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        req_json(&v, "GET", &with_params("/_cluster/health".into(), &v), None)
    })
}

// ── index admin ─────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__index_create(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let body = v.get("body").filter(|b| !b.is_null());
        let payload = body.map(|b| b.to_string());
        req_json(&v, "PUT", &format!("/{}", index), payload)
    })
}

#[no_mangle]
pub extern "C" fn search__index_delete(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        req_json(&v, "DELETE", &format!("/{}", index), None)
    })
}

#[no_mangle]
pub extern "C" fn search__index_exists(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        Ok(json!({"value": req_exists(&v, &format!("/{}", index))?}))
    })
}

#[no_mangle]
pub extern "C" fn search__index_list(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        req_json(&v, "GET", "/_cat/indices?format=json", None)
    })
}

#[no_mangle]
pub extern "C" fn search__index_refresh(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(&v, "POST", &format!("/{}/_refresh", index), None)
    })
}

#[no_mangle]
pub extern "C" fn search__index_open(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        req_json(&v, "POST", &format!("/{}/_open", index), None)
    })
}

#[no_mangle]
pub extern "C" fn search__index_close(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        req_json(&v, "POST", &format!("/{}/_close", index), None)
    })
}

#[no_mangle]
pub extern "C" fn search__index_stats(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(&v, "GET", &format!("/{}/_stats", index), None)
    })
}

#[no_mangle]
pub extern "C" fn search__forcemerge(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(
            &v,
            "POST",
            &with_params(format!("/{}/_forcemerge", index), &v),
            None,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__flush(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(
            &v,
            "POST",
            &with_params(format!("/{}/_flush", index), &v),
            None,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__clear_cache(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(
            &v,
            "POST",
            &with_params(format!("/{}/_cache/clear", index), &v),
            None,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__settings_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(&v, "GET", &format!("/{}/_settings", index), None)
    })
}

#[no_mangle]
pub extern "C" fn search__settings_update(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let body = str_or_obj(&v, "body")?;
        req_json(&v, "PUT", &format!("/{}/_settings", index), Some(body))
    })
}

#[no_mangle]
pub extern "C" fn search__mapping_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(&v, "GET", &format!("/{}/_mapping", index), None)
    })
}

#[no_mangle]
pub extern "C" fn search__mapping_put(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let body = str_or_obj(&v, "body")?;
        req_json(&v, "PUT", &format!("/{}/_mapping", index), Some(body))
    })
}

// ── aliases ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__alias_add(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let alias = str_field(&v, "alias")?;
        let body = json!({"actions": [{"add": {"index": index, "alias": alias}}]});
        req_json(&v, "POST", "/_aliases", Some(body.to_string()))
    })
}

#[no_mangle]
pub extern "C" fn search__alias_remove(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let alias = str_field(&v, "alias")?;
        let body = json!({"actions": [{"remove": {"index": index, "alias": alias}}]});
        req_json(&v, "POST", "/_aliases", Some(body.to_string()))
    })
}

#[no_mangle]
pub extern "C" fn search__alias_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(&v, "GET", &format!("/{}/_alias", index), None)
    })
}

// ── documents ───────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__doc_index(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let doc = str_or_obj(&v, "document")?;
        match opt_str(&v, "id") {
            Some(id) => req_json(
                &v,
                "PUT",
                &with_params(format!("/{}/_doc/{}", index, id), &v),
                Some(doc),
            ),
            None => req_json(
                &v,
                "POST",
                &with_params(format!("/{}/_doc", index), &v),
                Some(doc),
            ),
        }
    })
}

#[no_mangle]
pub extern "C" fn search__doc_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let id = str_field(&v, "id")?;
        req_json(&v, "GET", &format!("/{}/_doc/{}", index, id), None)
    })
}

#[no_mangle]
pub extern "C" fn search__doc_exists(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let id = str_field(&v, "id")?;
        Ok(json!({"value": req_exists(&v, &format!("/{}/_doc/{}", index, id))?}))
    })
}

#[no_mangle]
pub extern "C" fn search__doc_update(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let id = str_field(&v, "id")?;
        let body = str_or_obj(&v, "body")?;
        req_json(
            &v,
            "POST",
            &with_params(format!("/{}/_update/{}", index, id), &v),
            Some(body),
        )
    })
}

#[no_mangle]
pub extern "C" fn search__doc_delete(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let id = str_field(&v, "id")?;
        req_json(
            &v,
            "DELETE",
            &with_params(format!("/{}/_doc/{}", index, id), &v),
            None,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__mget(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let ids = string_vec(v.get("ids").unwrap_or(&Value::Null))?;
        let body = json!({ "ids": ids });
        let path = match opt_str(&v, "index") {
            Some(i) => format!("/{}/_mget", i),
            None => "/_mget".to_string(),
        };
        req_json(&v, "POST", &path, Some(body.to_string()))
    })
}

#[no_mangle]
pub extern "C" fn search__bulk(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let ops = v
            .get("ops")
            .and_then(|o| o.as_array())
            .ok_or_else(|| anyhow!("missing ops array"))?;
        let ndjson = build_ndjson(ops)?;
        let path = match opt_str(&v, "index") {
            Some(i) => format!("/{}/_bulk", i),
            None => "/_bulk".to_string(),
        };
        req_json(&v, "POST", &path, Some(ndjson))
    })
}

// ── search ──────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__search(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(
            &v,
            "POST",
            &with_params(format!("/{}/_search", index), &v),
            search_body(&v),
        )
    })
}

#[no_mangle]
pub extern "C" fn search__count(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(&v, "POST", &format!("/{}/_count", index), search_body(&v))
    })
}

#[no_mangle]
pub extern "C" fn search__msearch(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let lines = v
            .get("searches")
            .and_then(|o| o.as_array())
            .ok_or_else(|| anyhow!("missing searches array"))?;
        let mut body = String::new();
        for pair in lines {
            // each entry is {header:{...}, body:{...}}
            let header = pair.get("header").cloned().unwrap_or(json!({}));
            let q = pair.get("body").cloned().unwrap_or(json!({}));
            body.push_str(&header.to_string());
            body.push('\n');
            body.push_str(&q.to_string());
            body.push('\n');
        }
        req_json(&v, "POST", "/_msearch", Some(body))
    })
}

/// `GET /{index}/_search_shards` — which indices/shards a search would hit,
/// for routing/preference diagnostics. `params` forwards `routing`,
/// `preference`, etc.
#[no_mangle]
pub extern "C" fn search__search_shards(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        req_json(
            &v,
            "GET",
            &with_params(format!("/{}/_search_shards", index), &v),
            None,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__scroll_start(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        let keep = opt_str(&v, "scroll").unwrap_or("1m");
        req_json(
            &v,
            "POST",
            &format!("/{}/_search?scroll={}", index, urlencode(keep)),
            search_body(&v),
        )
    })
}

#[no_mangle]
pub extern "C" fn search__scroll_next(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let scroll_id = str_field(&v, "scroll_id")?;
        let keep = opt_str(&v, "scroll").unwrap_or("1m");
        let body = json!({"scroll": keep, "scroll_id": scroll_id});
        req_json(&v, "POST", "/_search/scroll", Some(body.to_string()))
    })
}

#[no_mangle]
pub extern "C" fn search__scroll_clear(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let scroll_id = str_field(&v, "scroll_id")?;
        let body = json!({"scroll_id": [scroll_id]});
        req_json(&v, "DELETE", "/_search/scroll", Some(body.to_string()))
    })
}

#[no_mangle]
pub extern "C" fn search__delete_by_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let body = str_or_obj(&v, "query")?;
        req_json(
            &v,
            "POST",
            &with_params(format!("/{}/_delete_by_query", index), &v),
            Some(body),
        )
    })
}

#[no_mangle]
pub extern "C" fn search__update_by_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let body = v
            .get("body")
            .filter(|b| !b.is_null())
            .map(|b| b.to_string());
        req_json(
            &v,
            "POST",
            &with_params(format!("/{}/_update_by_query", index), &v),
            body,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__reindex(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let source = str_field(&v, "source")?;
        let dest = str_field(&v, "dest")?;
        let body = json!({"source": {"index": source}, "dest": {"index": dest}});
        req_json(
            &v,
            "POST",
            &with_params("/_reindex".into(), &v),
            Some(body.to_string()),
        )
    })
}

#[no_mangle]
pub extern "C" fn search__analyze(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let body = str_or_obj(&v, "body")?;
        let path = match opt_str(&v, "index") {
            Some(i) => format!("/{}/_analyze", i),
            None => "/_analyze".to_string(),
        };
        req_json(&v, "POST", &path, Some(body))
    })
}

#[no_mangle]
pub extern "C" fn search__explain(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let id = str_field(&v, "id")?;
        let body = str_or_obj(&v, "query")?;
        req_json(
            &v,
            "POST",
            &format!("/{}/_explain/{}", index, id),
            Some(body),
        )
    })
}

#[no_mangle]
pub extern "C" fn search__cat(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let what = str_field(&v, "what")?;
        let path = format!("/_cat/{}?format=json", what.trim_start_matches('/'));
        req_json(&v, "GET", &path, None)
    })
}

/// Generic escape hatch: caller supplies `method`, `path`, optional `body`.
#[no_mangle]
pub extern "C" fn search__raw(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let method = opt_str(&v, "method").unwrap_or("GET").to_uppercase();
        let path = str_field(&v, "path")?;
        let body = v.get("body").filter(|b| !b.is_null()).map(|b| match b {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        });
        req_json(&v, &method, path, body)
    })
}

// ── aggregations runner ─────────────────────────────────────────────────────

/// Run an aggregations-only search: `{ size: 0, aggs: <aggs>, query?: <query> }`.
/// Returns the cluster response (the `aggregations` block lives under
/// `.aggregations`). Pass `size` to also return hits.
#[no_mangle]
pub extern "C" fn search__search_aggs(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        let aggs = v
            .get("aggs")
            .cloned()
            .ok_or_else(|| anyhow!("missing aggs"))?;
        let mut body = serde_json::Map::new();
        let size = v.get("size").cloned().unwrap_or(json!(0));
        body.insert("size".into(), size);
        body.insert("aggs".into(), aggs);
        if let Some(q) = v.get("query").filter(|x| !x.is_null()) {
            body.insert("query".into(), q.clone());
        }
        req_json(
            &v,
            "POST",
            &format!("/{}/_search", index),
            Some(Value::Object(body).to_string()),
        )
    })
}

// ── field caps + term vectors ───────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__field_caps(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        let fields = string_vec(v.get("fields").unwrap_or(&Value::Null))?;
        let path = format!(
            "/{}/_field_caps?fields={}",
            index,
            urlencode(&fields.join(","))
        );
        req_json(&v, "GET", &path, None)
    })
}

#[no_mangle]
pub extern "C" fn search__termvectors(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let body = v
            .get("body")
            .filter(|b| !b.is_null())
            .map(|b| b.to_string());
        let path = match opt_str(&v, "id") {
            Some(id) => format!("/{}/_termvectors/{}", index, id),
            None => format!("/{}/_termvectors", index),
        };
        req_json(&v, "POST", &path, body)
    })
}

#[no_mangle]
pub extern "C" fn search__mtermvectors(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let body = str_or_obj(&v, "body")?;
        let path = match opt_str(&v, "index") {
            Some(i) => format!("/{}/_mtermvectors", i),
            None => "/_mtermvectors".to_string(),
        };
        req_json(&v, "POST", &path, Some(body))
    })
}

// ── index + component templates ─────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__index_template_put(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        let body = str_or_obj(&v, "body")?;
        req_json(&v, "PUT", &format!("/_index_template/{}", name), Some(body))
    })
}

#[no_mangle]
pub extern "C" fn search__index_template_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let path = match opt_str(&v, "name") {
            Some(n) => format!("/_index_template/{}", n),
            None => "/_index_template".to_string(),
        };
        req_json(&v, "GET", &path, None)
    })
}

#[no_mangle]
pub extern "C" fn search__index_template_delete(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        req_json(&v, "DELETE", &format!("/_index_template/{}", name), None)
    })
}

#[no_mangle]
pub extern "C" fn search__index_template_exists(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        Ok(json!({"value": req_exists(&v, &format!("/_index_template/{}", name))?}))
    })
}

#[no_mangle]
pub extern "C" fn search__component_template_put(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        let body = str_or_obj(&v, "body")?;
        req_json(
            &v,
            "PUT",
            &format!("/_component_template/{}", name),
            Some(body),
        )
    })
}

#[no_mangle]
pub extern "C" fn search__component_template_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let path = match opt_str(&v, "name") {
            Some(n) => format!("/_component_template/{}", n),
            None => "/_component_template".to_string(),
        };
        req_json(&v, "GET", &path, None)
    })
}

#[no_mangle]
pub extern "C" fn search__component_template_delete(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        req_json(
            &v,
            "DELETE",
            &format!("/_component_template/{}", name),
            None,
        )
    })
}

// ── ingest pipelines ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__ingest_put(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let id = str_field(&v, "id")?;
        let body = str_or_obj(&v, "body")?;
        req_json(&v, "PUT", &format!("/_ingest/pipeline/{}", id), Some(body))
    })
}

#[no_mangle]
pub extern "C" fn search__ingest_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let path = match opt_str(&v, "id") {
            Some(id) => format!("/_ingest/pipeline/{}", id),
            None => "/_ingest/pipeline".to_string(),
        };
        req_json(&v, "GET", &path, None)
    })
}

#[no_mangle]
pub extern "C" fn search__ingest_delete(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let id = str_field(&v, "id")?;
        req_json(&v, "DELETE", &format!("/_ingest/pipeline/{}", id), None)
    })
}

#[no_mangle]
pub extern "C" fn search__ingest_simulate(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let body = str_or_obj(&v, "body")?;
        let path = match opt_str(&v, "id") {
            Some(id) => format!("/_ingest/pipeline/{}/_simulate", id),
            None => "/_ingest/pipeline/_simulate".to_string(),
        };
        req_json(&v, "POST", &path, Some(body))
    })
}

// ── point in time ───────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__pit_open(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = str_field(&v, "index")?;
        let keep = opt_str(&v, "keep_alive").unwrap_or("1m");
        req_json(
            &v,
            "POST",
            &format!("/{}/_pit?keep_alive={}", index, urlencode(keep)),
            None,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__pit_close(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let id = str_field(&v, "id")?;
        let body = json!({ "id": id });
        req_json(&v, "DELETE", "/_pit", Some(body.to_string()))
    })
}

// ── snapshot + repository admin ─────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__repo_create(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let repo = str_field(&v, "repo")?;
        let body = str_or_obj(&v, "body")?;
        req_json(&v, "PUT", &format!("/_snapshot/{}", repo), Some(body))
    })
}

#[no_mangle]
pub extern "C" fn search__repo_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let repo = opt_str(&v, "repo").unwrap_or("_all");
        req_json(&v, "GET", &format!("/_snapshot/{}", repo), None)
    })
}

#[no_mangle]
pub extern "C" fn search__repo_delete(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let repo = str_field(&v, "repo")?;
        req_json(&v, "DELETE", &format!("/_snapshot/{}", repo), None)
    })
}

#[no_mangle]
pub extern "C" fn search__snapshot_create(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let repo = str_field(&v, "repo")?;
        let snapshot = str_field(&v, "snapshot")?;
        let body = v
            .get("body")
            .filter(|b| !b.is_null())
            .map(|b| b.to_string());
        req_json(
            &v,
            "PUT",
            &with_params(format!("/_snapshot/{}/{}", repo, snapshot), &v),
            body,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__snapshot_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let repo = str_field(&v, "repo")?;
        let snapshot = opt_str(&v, "snapshot").unwrap_or("_all");
        req_json(
            &v,
            "GET",
            &format!("/_snapshot/{}/{}", repo, snapshot),
            None,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__snapshot_delete(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let repo = str_field(&v, "repo")?;
        let snapshot = str_field(&v, "snapshot")?;
        req_json(
            &v,
            "DELETE",
            &format!("/_snapshot/{}/{}", repo, snapshot),
            None,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__snapshot_restore(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let repo = str_field(&v, "repo")?;
        let snapshot = str_field(&v, "snapshot")?;
        let body = v
            .get("body")
            .filter(|b| !b.is_null())
            .map(|b| b.to_string());
        req_json(
            &v,
            "POST",
            &format!("/_snapshot/{}/{}/_restore", repo, snapshot),
            body,
        )
    })
}

// ── tasks ───────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__tasks_list(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        req_json(&v, "GET", &with_params("/_tasks".into(), &v), None)
    })
}

#[no_mangle]
pub extern "C" fn search__tasks_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let task_id = str_field(&v, "task_id")?;
        req_json(&v, "GET", &format!("/_tasks/{}", task_id), None)
    })
}

#[no_mangle]
pub extern "C" fn search__tasks_cancel(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let task_id = str_field(&v, "task_id")?;
        req_json(&v, "POST", &format!("/_tasks/{}/_cancel", task_id), None)
    })
}

// ── cluster + nodes ─────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__cluster_stats(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| req_json(&v, "GET", "/_cluster/stats", None))
}

#[no_mangle]
pub extern "C" fn search__cluster_state(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| req_json(&v, "GET", "/_cluster/state", None))
}

#[no_mangle]
pub extern "C" fn search__cluster_settings_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        req_json(
            &v,
            "GET",
            &with_params("/_cluster/settings".into(), &v),
            None,
        )
    })
}

#[no_mangle]
pub extern "C" fn search__cluster_settings_put(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let body = str_or_obj(&v, "body")?;
        req_json(&v, "PUT", "/_cluster/settings", Some(body))
    })
}

#[no_mangle]
pub extern "C" fn search__nodes_info(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| req_json(&v, "GET", "/_nodes", None))
}

#[no_mangle]
pub extern "C" fn search__nodes_stats(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| req_json(&v, "GET", "/_nodes/stats", None))
}

#[no_mangle]
pub extern "C" fn search__pending_tasks(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        req_json(&v, "GET", "/_cluster/pending_tasks", None)
    })
}

#[no_mangle]
pub extern "C" fn search__allocation_explain(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let body = v
            .get("body")
            .filter(|b| !b.is_null())
            .map(|b| b.to_string());
        req_json(&v, "POST", "/_cluster/allocation/explain", body)
    })
}

// ── stored scripts + search templates ───────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__script_put(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let id = str_field(&v, "id")?;
        let body = str_or_obj(&v, "body")?;
        req_json(&v, "PUT", &format!("/_scripts/{}", id), Some(body))
    })
}

#[no_mangle]
pub extern "C" fn search__script_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let id = str_field(&v, "id")?;
        req_json(&v, "GET", &format!("/_scripts/{}", id), None)
    })
}

#[no_mangle]
pub extern "C" fn search__script_delete(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let id = str_field(&v, "id")?;
        req_json(&v, "DELETE", &format!("/_scripts/{}", id), None)
    })
}

#[no_mangle]
pub extern "C" fn search__search_template(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let index = opt_str(&v, "index").unwrap_or("_all");
        let body = str_or_obj(&v, "body")?;
        req_json(
            &v,
            "POST",
            &format!("/{}/_search/template", index),
            Some(body),
        )
    })
}

#[no_mangle]
pub extern "C" fn search__render_template(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let body = str_or_obj(&v, "body")?;
        req_json(&v, "POST", "/_render/template", Some(body))
    })
}

// ── pure query-DSL builders (no network) ────────────────────────────────────
//
// Each builder returns a *bare query clause* (e.g. `{"match":{…}}`), not a full
// `{"query":…}` body. Bare clauses nest directly inside `bool`, `constant_score`,
// `nested`, etc.; `Search::search` / `::count` auto-wrap a top-level clause as
// `{"query": clause}` (see `search_body`).

#[no_mangle]
pub extern "C" fn search__match_all(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"value": {"match_all": {}}})))
}

#[no_mangle]
pub extern "C" fn search__match_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let value = v.get("value").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"match": {field: value}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__match_phrase(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let value = v.get("value").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"match_phrase": {field: value}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__match_phrase_prefix(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let value = v.get("value").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"match_phrase_prefix": {field: value}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__term_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let value = v.get("value").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"term": {field: {"value": value}}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__terms_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let values = v.get("values").cloned().unwrap_or(json!([]));
        Ok(json!({"value": {"terms": {field: values}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__range_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let mut spec = serde_json::Map::new();
        for k in ["gt", "gte", "lt", "lte", "format", "time_zone", "boost"] {
            if let Some(val) = v.get(k).filter(|x| !x.is_null()) {
                spec.insert(k.to_string(), val.clone());
            }
        }
        Ok(json!({"value": {"range": {field: spec}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__prefix_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let value = v.get("value").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"prefix": {field: {"value": value}}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__wildcard_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let value = v.get("value").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"wildcard": {field: {"value": value}}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__regexp_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let value = v.get("value").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"regexp": {field: {"value": value}}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__fuzzy_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let value = v.get("value").cloned().unwrap_or(Value::Null);
        let mut spec = serde_json::Map::new();
        spec.insert("value".into(), value);
        if let Some(f) = v.get("fuzziness").filter(|x| !x.is_null()) {
            spec.insert("fuzziness".into(), f.clone());
        }
        Ok(json!({"value": {"fuzzy": {field: spec}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__exists_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        Ok(json!({"value": {"exists": {"field": field}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__ids_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let values = string_vec(v.get("values").unwrap_or(&Value::Null))?;
        Ok(json!({"value": {"ids": {"values": values}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__query_string(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let query = str_field(&v, "query")?;
        let mut qs = serde_json::Map::new();
        qs.insert("query".into(), json!(query));
        if let Some(f) = v.get("default_field").filter(|x| !x.is_null()) {
            qs.insert("default_field".into(), f.clone());
        }
        if let Some(f) = v.get("fields").filter(|x| !x.is_null()) {
            qs.insert("fields".into(), f.clone());
        }
        Ok(json!({"value": {"query_string": qs}}))
    })
}

#[no_mangle]
pub extern "C" fn search__simple_query_string(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let query = str_field(&v, "query")?;
        let mut qs = serde_json::Map::new();
        qs.insert("query".into(), json!(query));
        if let Some(f) = v.get("fields").filter(|x| !x.is_null()) {
            qs.insert("fields".into(), f.clone());
        }
        Ok(json!({"value": {"simple_query_string": qs}}))
    })
}

#[no_mangle]
pub extern "C" fn search__multi_match(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let query = v.get("query").cloned().unwrap_or(Value::Null);
        let fields = v.get("fields").cloned().unwrap_or(json!([]));
        let mut mm = serde_json::Map::new();
        mm.insert("query".into(), query);
        mm.insert("fields".into(), fields);
        if let Some(t) = v.get("type").filter(|x| !x.is_null()) {
            mm.insert("type".into(), t.clone());
        }
        Ok(json!({"value": {"multi_match": mm}}))
    })
}

#[no_mangle]
pub extern "C" fn search__geo_distance_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let distance = str_field(&v, "distance")?;
        let lat = v.get("lat").cloned().unwrap_or(Value::Null);
        let lon = v.get("lon").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"geo_distance": {
            "distance": distance,
            field: {"lat": lat, "lon": lon},
        }}}))
    })
}

/// geo_bounding_box query — docs whose `field` falls inside the box defined by
/// `top_left` / `bottom_right` (each a `{lat, lon}` object or a geohash/string
/// point, passed through verbatim).
#[no_mangle]
pub extern "C" fn search__geo_bounding_box_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let top_left = v.get("top_left").cloned().unwrap_or(Value::Null);
        let bottom_right = v.get("bottom_right").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"geo_bounding_box": {
            field: {"top_left": top_left, "bottom_right": bottom_right},
        }}}))
    })
}

/// function_score query — wrap a `query` clause with `functions` that modify
/// the score. Optional `score_mode` / `boost_mode` passed through when present.
#[no_mangle]
pub extern "C" fn search__function_score_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let mut fs = serde_json::Map::new();
        if let Some(q) = v.get("query").filter(|x| !x.is_null()) {
            fs.insert("query".into(), q.clone());
        }
        if let Some(f) = v.get("functions").filter(|x| !x.is_null()) {
            fs.insert("functions".into(), f.clone());
        }
        for k in [
            "score_mode",
            "boost_mode",
            "min_score",
            "max_boost",
            "boost",
        ] {
            if let Some(val) = v.get(k).filter(|x| !x.is_null()) {
                fs.insert(k.to_string(), val.clone());
            }
        }
        Ok(json!({"value": {"function_score": fs}}))
    })
}

/// more_like_this query — find documents similar to `like` (a string, doc
/// reference, or array of either) across `fields`. Optional tuning knobs
/// (`min_term_freq`, `max_query_terms`, `min_doc_freq`, `minimum_should_match`)
/// pass through when present.
#[no_mangle]
pub extern "C" fn search__more_like_this_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let mut mlt = serde_json::Map::new();
        if let Some(f) = v.get("fields").filter(|x| !x.is_null()) {
            mlt.insert("fields".into(), f.clone());
        }
        let like = v.get("like").cloned().unwrap_or(Value::Null);
        mlt.insert("like".into(), like);
        for k in [
            "min_term_freq",
            "max_query_terms",
            "min_doc_freq",
            "max_doc_freq",
            "minimum_should_match",
        ] {
            if let Some(val) = v.get(k).filter(|x| !x.is_null()) {
                mlt.insert(k.to_string(), val.clone());
            }
        }
        Ok(json!({"value": {"more_like_this": mlt}}))
    })
}

#[no_mangle]
pub extern "C" fn search__nested_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let path = str_field(&v, "path")?;
        let query = v.get("query").cloned().unwrap_or(json!({"match_all": {}}));
        Ok(json!({"value": {"nested": {"path": path, "query": query}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__constant_score(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let filter = v.get("filter").cloned().unwrap_or(json!({"match_all": {}}));
        let mut cs = serde_json::Map::new();
        cs.insert("filter".into(), filter);
        if let Some(b) = v.get("boost").filter(|x| !x.is_null()) {
            cs.insert("boost".into(), b.clone());
        }
        Ok(json!({"value": {"constant_score": cs}}))
    })
}

#[no_mangle]
pub extern "C" fn search__dis_max(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let queries = v.get("queries").cloned().unwrap_or(json!([]));
        let mut dm = serde_json::Map::new();
        dm.insert("queries".into(), queries);
        if let Some(t) = v.get("tie_breaker").filter(|x| !x.is_null()) {
            dm.insert("tie_breaker".into(), t.clone());
        }
        Ok(json!({"value": {"dis_max": dm}}))
    })
}

#[no_mangle]
pub extern "C" fn search__bool_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let mut b = serde_json::Map::new();
        for clause in ["must", "should", "must_not", "filter"] {
            if let Some(arr) = v.get(clause).filter(|x| !x.is_null()) {
                b.insert(clause.to_string(), normalize_clause(arr));
            }
        }
        if let Some(m) = v.get("minimum_should_match").filter(|x| !x.is_null()) {
            b.insert("minimum_should_match".into(), m.clone());
        }
        Ok(json!({"value": {"bool": b}}))
    })
}

// ── full-body composers + aggregations ──────────────────────────────────────

/// Compose a full search body from optional parts: `query` (a bare clause —
/// auto-wrapped), `aggs`, `sort`, `size`, `from`, `_source`, `highlight`,
/// `track_total_hits`, `search_after`. Returns the assembled body ready for
/// `Search::search`.
#[no_mangle]
pub extern "C" fn search__query_body(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let mut body = serde_json::Map::new();
        if let Some(q) = v.get("query").filter(|x| !x.is_null()) {
            body.insert("query".into(), q.clone());
        }
        for k in [
            "aggs",
            "sort",
            "size",
            "from",
            "_source",
            "highlight",
            "track_total_hits",
            "search_after",
            "collapse",
        ] {
            if let Some(val) = v.get(k).filter(|x| !x.is_null()) {
                body.insert(k.to_string(), val.clone());
            }
        }
        Ok(json!({ "value": Value::Object(body) }))
    })
}

/// Build a single aggregation definition `{ <type>: <spec> }` for nesting under
/// a search body's `aggs`. `agg_type` names the aggregation (`terms`, `avg`,
/// `date_histogram`, …); the remaining fields become its spec.
#[no_mangle]
pub extern "C" fn search__agg(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let agg_type = str_field(&v, "agg_type")?;
        let spec = v.get("spec").cloned().unwrap_or(json!({}));
        Ok(json!({ "value": { agg_type: spec } }))
    })
}

/// terms bucket aggregation on `field` (optional `size`).
#[no_mangle]
pub extern "C" fn search__agg_terms(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let mut spec = serde_json::Map::new();
        spec.insert("field".into(), json!(field));
        if let Some(s) = v.get("size").filter(|x| !x.is_null()) {
            spec.insert("size".into(), s.clone());
        }
        Ok(json!({"value": {"terms": spec}}))
    })
}

/// Single-field metric aggregations sharing the `{ <metric>: { field } }` shape.
fn metric_agg(v: &Value, metric: &str) -> Result<Value> {
    let field = str_field(v, "field")?;
    Ok(json!({"value": {metric: {"field": field}}}))
}

#[no_mangle]
pub extern "C" fn search__agg_avg(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| metric_agg(&v, "avg"))
}

#[no_mangle]
pub extern "C" fn search__agg_sum(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| metric_agg(&v, "sum"))
}

#[no_mangle]
pub extern "C" fn search__agg_min(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| metric_agg(&v, "min"))
}

#[no_mangle]
pub extern "C" fn search__agg_max(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| metric_agg(&v, "max"))
}

#[no_mangle]
pub extern "C" fn search__agg_stats(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| metric_agg(&v, "stats"))
}

#[no_mangle]
pub extern "C" fn search__agg_extended_stats(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| metric_agg(&v, "extended_stats"))
}

#[no_mangle]
pub extern "C" fn search__agg_cardinality(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| metric_agg(&v, "cardinality"))
}

#[no_mangle]
pub extern "C" fn search__agg_value_count(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| metric_agg(&v, "value_count"))
}

#[no_mangle]
pub extern "C" fn search__agg_percentiles(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let mut spec = serde_json::Map::new();
        spec.insert("field".into(), json!(field));
        if let Some(p) = v.get("percents").filter(|x| !x.is_null()) {
            spec.insert("percents".into(), p.clone());
        }
        Ok(json!({"value": {"percentiles": spec}}))
    })
}

#[no_mangle]
pub extern "C" fn search__agg_histogram(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let interval = v.get("interval").cloned().unwrap_or(Value::Null);
        Ok(json!({"value": {"histogram": {"field": field, "interval": interval}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__agg_date_histogram(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let mut spec = serde_json::Map::new();
        spec.insert("field".into(), json!(field));
        // ES 8 uses calendar_interval / fixed_interval; accept either.
        for k in ["calendar_interval", "fixed_interval", "format", "time_zone"] {
            if let Some(val) = v.get(k).filter(|x| !x.is_null()) {
                spec.insert(k.to_string(), val.clone());
            }
        }
        Ok(json!({"value": {"date_histogram": spec}}))
    })
}

#[no_mangle]
pub extern "C" fn search__agg_range(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let ranges = v.get("ranges").cloned().unwrap_or(json!([]));
        Ok(json!({"value": {"range": {"field": field, "ranges": ranges}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__agg_filter(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let filter = v.get("filter").cloned().unwrap_or(json!({"match_all": {}}));
        Ok(json!({"value": {"filter": filter}}))
    })
}

#[no_mangle]
pub extern "C" fn search__agg_missing(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        Ok(json!({"value": {"missing": {"field": field}}}))
    })
}

#[no_mangle]
pub extern "C" fn search__agg_nested(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let path = str_field(&v, "path")?;
        Ok(json!({"value": {"nested": {"path": path}}}))
    })
}

/// top_hits metric aggregation — return the top matching documents per bucket.
/// `size`, `sort`, `_source`, and `from` pass through when present.
#[no_mangle]
pub extern "C" fn search__agg_top_hits(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let mut spec = serde_json::Map::new();
        for k in ["size", "sort", "_source", "from"] {
            if let Some(val) = v.get(k).filter(|x| !x.is_null()) {
                spec.insert(k.to_string(), val.clone());
            }
        }
        Ok(json!({"value": {"top_hits": spec}}))
    })
}

/// global aggregation bucket — escapes the query scope so sub-aggs see every
/// document in the index, not just the search hits.
#[no_mangle]
pub extern "C" fn search__agg_global(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"value": {"global": {}}})))
}

// ── full-body field helpers ─────────────────────────────────────────────────

/// Build a sort clause `[{ field: { order } }]` (order defaults to "asc").
#[no_mangle]
pub extern "C" fn search__sort_clause(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let field = str_field(&v, "field")?;
        let order = opt_str(&v, "order").unwrap_or("asc");
        Ok(json!({"value": [{field: {"order": order}}]}))
    })
}

/// Build a highlight clause highlighting the given fields.
#[no_mangle]
pub extern "C" fn search__highlight_clause(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let fields = string_vec(v.get("fields").unwrap_or(&Value::Null))?;
        let mut hf = serde_json::Map::new();
        for f in fields {
            hf.insert(f, json!({}));
        }
        Ok(json!({"value": {"fields": hf}}))
    })
}

#[no_mangle]
pub extern "C" fn search__bulk_ndjson(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let ops = v
            .get("ops")
            .and_then(|o| o.as_array())
            .ok_or_else(|| anyhow!("missing ops array"))?;
        Ok(json!({"value": build_ndjson(ops)?}))
    })
}

/// Escape the Lucene query-string special characters so a user term is taken
/// literally by `query_string` / `simple_query_string`. Mirrors the set
/// listed in the Elasticsearch query-string-syntax docs:
/// `+ - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /`.
#[no_mangle]
pub extern "C" fn search__escape_query_string(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({"value": escape_lucene(s)}))
    })
}

// ── pure URL helpers ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn search__build_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = conn_from_opts(&v);
        Ok(json!({"value": key.base}))
    })
}

#[no_mangle]
pub extern "C" fn search__parse_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let url = str_field(&v, "url")?;
        Ok(parse_url(url))
    })
}

/// Replace the userinfo (`user:pass@`) in a URL with `***` for safe logging.
#[no_mangle]
pub extern "C" fn search__redact_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let url = str_field(&v, "url")?;
        Ok(json!({"value": redact_url(url)}))
    })
}

/// Validate an ES/OpenSearch index name against the engine's hard naming rules.
/// Returns `{name, valid, reason}`. Pure — no request.
#[no_mangle]
pub extern "C" fn search__valid_index_name(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        let (valid, reason) = valid_index_name(name);
        Ok(json!({"name": name, "valid": valid, "reason": reason}))
    })
}

// ── shared pure logic (unit-tested below) ───────────────────────────────────

/// Read a field that may arrive as either a JSON string or an object/array,
/// returning the serialized JSON string to send as the request body.
fn str_or_obj(v: &Value, k: &str) -> Result<String> {
    match v.get(k) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Null) | None => Err(anyhow!("missing {}", k)),
        Some(other) => Ok(other.to_string()),
    }
}

fn string_vec(v: &Value) -> Result<Vec<String>> {
    match v {
        Value::Array(a) => a
            .iter()
            .map(|x| {
                x.as_str()
                    .map(String::from)
                    .ok_or_else(|| anyhow!("non-string in array"))
            })
            .collect(),
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Null => Ok(Vec::new()),
        _ => Err(anyhow!("expected string or array of strings")),
    }
}

/// A bool clause may be given as a single query object or an array of them;
/// normalize a single object to a one-element array (ES accepts both, but a
/// uniform array keeps the builder predictable).
fn normalize_clause(v: &Value) -> Value {
    match v {
        Value::Array(_) => v.clone(),
        other => json!([other]),
    }
}

/// Build an Elasticsearch `_bulk` NDJSON body from an array of op objects.
/// Each op is `{action: "index"|"create"|"update"|"delete", index?, id?,
/// document?, doc?}`. Action+meta line, then (for non-delete) the source
/// line. Trailing newline is required by the `_bulk` API.
fn build_ndjson(ops: &[Value]) -> Result<String> {
    let mut out = String::new();
    for op in ops {
        let action = op
            .get("action")
            .and_then(|a| a.as_str())
            .ok_or_else(|| anyhow!("bulk op missing action"))?;
        let mut meta = serde_json::Map::new();
        for k in ["index", "id"] {
            if let Some(val) = op.get(k).filter(|x| !x.is_null()) {
                let mk = if k == "index" { "_index" } else { "_id" };
                meta.insert(mk.to_string(), val.clone());
            }
        }
        let action_line = json!({ action: Value::Object(meta) });
        out.push_str(&action_line.to_string());
        out.push('\n');
        match action {
            "delete" => {}
            "update" => {
                let doc = op
                    .get("doc")
                    .or_else(|| op.get("document"))
                    .cloned()
                    .unwrap_or(json!({}));
                out.push_str(&json!({ "doc": doc }).to_string());
                out.push('\n');
            }
            _ => {
                let doc = op
                    .get("document")
                    .or_else(|| op.get("doc"))
                    .cloned()
                    .ok_or_else(|| anyhow!("bulk {} op missing document", action))?;
                out.push_str(&doc.to_string());
                out.push('\n');
            }
        }
    }
    Ok(out)
}

fn escape_lucene(s: &str) -> String {
    const SPECIAL: &[char] = &[
        '+', '-', '=', '>', '<', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':',
        '\\', '/', '&', '|',
    ];
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if SPECIAL.contains(&ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Validate an Elasticsearch / OpenSearch index name against the engine's hard
/// rules: lowercase only; no `\ / * ? " < > | `, space, comma, or `#`; must not
/// start with `-`, `_`, or `+`; must not be `.` or `..`; at most 255 bytes.
/// Returns `(valid, reason)`.
fn valid_index_name(name: &str) -> (bool, Option<&'static str>) {
    const FORBIDDEN: &[char] = &['\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#'];
    let reason = if name.is_empty() {
        Some("must not be empty")
    } else if name == "." || name == ".." {
        Some("must not be '.' or '..'")
    } else if name.starts_with('-') || name.starts_with('_') || name.starts_with('+') {
        Some("must not start with '-', '_', or '+'")
    } else if name.chars().any(|c| c.is_uppercase()) {
        Some("must be lowercase")
    } else if name.chars().any(|c| FORBIDDEN.contains(&c)) {
        Some("must not contain a space, comma, '#', or any of: \\ / * ? \" < > |")
    } else if name.len() > 255 {
        Some("must be at most 255 bytes")
    } else {
        None
    };
    (reason.is_none(), reason)
}

/// Decompose `scheme://user:pass@host:port/path` into parts. Missing pieces
/// come back as null/defaults; the default port follows the scheme
/// (`https` → 9243-style clusters still default to 9200 here, callers can
/// override).
fn parse_url(url: &str) -> Value {
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (s.to_string(), r),
        None => ("http".to_string(), url),
    };
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{}", p)),
        None => (rest, String::new()),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (username, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (Some(u.to_string()), Some(p.to_string())),
            None => (Some(ui.to_string()), None),
        },
        None => (None, None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<i64>().ok()),
        None => (hostport.to_string(), None),
    };
    let tls = scheme == "https";
    json!({
        "scheme": scheme,
        "host": host,
        "port": port.unwrap_or(if tls { 443 } else { 9200 }),
        "username": username,
        "password": password,
        "path": if path.is_empty() { Value::Null } else { Value::String(path) },
        "tls": tls,
    })
}

fn redact_url(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((_, hostpart)) => format!("{}://***@{}", scheme, hostpart),
            None => url.to_string(),
        },
        None => url.to_string(),
    }
}

// ── unit tests for the pure logic ───────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_index_name_enforces_es_rules() {
        assert!(valid_index_name("books").0);
        assert!(valid_index_name("logs-2025.06.20").0);
        for (name, want) in [
            ("", "empty"),
            (".", "'.'"),
            ("..", "'.'"),
            ("_hidden", "start with"),
            ("-lead", "start with"),
            ("+plus", "start with"),
            ("Books", "lowercase"),
            ("a b", "space"),
            ("a/b", "\\"),
            ("a,b", "space"),
            ("a#b", "#"),
        ] {
            let (valid, reason) = valid_index_name(name);
            assert!(!valid, "{name:?} should be invalid");
            assert!(
                reason.unwrap().contains(want),
                "{name:?}: reason `{}` should mention `{want}`",
                reason.unwrap()
            );
        }
        assert!(!valid_index_name(&"a".repeat(256)).0);
        assert!(valid_index_name(&"a".repeat(255)).0);
    }

    #[test]
    fn ndjson_index_and_delete() {
        let ops = vec![
            json!({"action": "index", "index": "books", "id": "1", "document": {"t": "a"}}),
            json!({"action": "delete", "index": "books", "id": "2"}),
        ];
        let body = build_ndjson(&ops).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 3, "index(2 lines) + delete(1 line)");
        assert!(lines[0].contains("\"_index\":\"books\""));
        assert!(lines[1].contains("\"t\":\"a\""));
        assert!(lines[2].contains("\"delete\""));
        assert!(body.ends_with('\n'), "bulk body must end in newline");
    }

    #[test]
    fn ndjson_update_uses_doc_wrapper() {
        let ops = vec![json!({"action": "update", "index": "i", "id": "9", "doc": {"x": 1}})];
        let body = build_ndjson(&ops).unwrap();
        assert!(
            body.contains("\"doc\":{\"x\":1}"),
            "update wraps in doc: {body}"
        );
    }

    #[test]
    fn ndjson_missing_action_errors() {
        let ops = vec![json!({"index": "i"})];
        assert!(build_ndjson(&ops).is_err());
    }

    #[test]
    fn escape_lucene_escapes_specials() {
        assert_eq!(escape_lucene("a+b"), "a\\+b");
        assert_eq!(escape_lucene("(x)"), "\\(x\\)");
        assert_eq!(escape_lucene("plain"), "plain");
        assert_eq!(escape_lucene("a&&b"), "a\\&\\&b");
    }

    #[test]
    fn parse_url_full() {
        let v = parse_url("https://user:pw@es.example.com:9243/prefix");
        assert_eq!(v["scheme"], "https");
        assert_eq!(v["host"], "es.example.com");
        assert_eq!(v["port"], 9243);
        assert_eq!(v["username"], "user");
        assert_eq!(v["password"], "pw");
        assert_eq!(v["tls"], true);
        assert_eq!(v["path"], "/prefix");
    }

    #[test]
    fn parse_url_bare_host_defaults() {
        let v = parse_url("localhost");
        assert_eq!(v["scheme"], "http");
        assert_eq!(v["host"], "localhost");
        assert_eq!(v["port"], 9200);
        assert_eq!(v["username"], Value::Null);
        assert_eq!(v["tls"], false);
    }

    #[test]
    fn redact_strips_userinfo() {
        assert_eq!(
            redact_url("https://user:secret@host:9200"),
            "https://***@host:9200"
        );
        assert_eq!(redact_url("http://host:9200"), "http://host:9200");
    }

    #[test]
    fn conn_from_opts_builds_base_and_basic_auth() {
        let opts = json!({"host": "h", "port": 9200, "username": "u", "password": "p"});
        let k = conn_from_opts(&opts);
        assert_eq!(k.base, "http://h:9200");
        assert!(k.auth.starts_with("Basic "));
        let decoded = B64.decode(k.auth.trim_start_matches("Basic ")).unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "u:p");
    }

    #[test]
    fn conn_from_opts_url_trims_slash_and_api_key() {
        let opts = json!({"url": "https://es:9200/", "api_key": "AbC123"});
        let k = conn_from_opts(&opts);
        assert_eq!(k.base, "https://es:9200");
        assert_eq!(k.auth, "ApiKey AbC123");
    }

    #[test]
    fn urlencode_encodes_reserved() {
        assert_eq!(urlencode("a b/c"), "a%20b%2Fc");
        assert_eq!(urlencode("plain-_.~"), "plain-_.~");
    }

    #[test]
    fn search_body_wraps_bare_clause() {
        let v = json!({"query": {"match": {"t": "rust"}}});
        assert_eq!(
            search_body(&v).unwrap(),
            r#"{"query":{"match":{"t":"rust"}}}"#
        );
    }

    #[test]
    fn search_body_passes_full_body_through() {
        // a clause carrying a full-body key (aggs) is sent verbatim, not wrapped
        let v = json!({"query": {"size": 0, "aggs": {"x": {"avg": {"field": "n"}}}}});
        let body = search_body(&v).unwrap();
        assert!(body.contains("\"aggs\""));
        assert!(
            !body.contains("\"query\":{\"size\""),
            "must not double-wrap: {body}"
        );
    }

    #[test]
    fn search_body_explicit_body_field_wins() {
        let v = json!({"body": {"query": {"match_all": {}}}, "query": {"match": {"a": "b"}}});
        let body = search_body(&v).unwrap();
        assert!(body.contains("match_all"));
        assert!(!body.contains("\"a\""));
    }

    #[test]
    fn search_body_none_when_empty() {
        assert!(search_body(&json!({})).is_none());
    }

    #[test]
    fn metric_agg_shape() {
        assert_eq!(
            metric_agg(&json!({"field": "price"}), "avg").unwrap(),
            json!({"value": {"avg": {"field": "price"}}})
        );
        assert!(metric_agg(&json!({}), "sum").is_err());
    }

    /// Drive a `search__*` builder export through the same JSON-in/JSON-out FFI
    /// contract stryke uses, returning the parsed response so the unit tests
    /// assert on the exact clause shape rather than re-deriving it.
    fn call_ffi(f: extern "C" fn(*const c_char) -> *const c_char, args: Value) -> Value {
        let input = CString::new(args.to_string()).unwrap();
        let out_ptr = f(input.as_ptr());
        let parsed = unsafe {
            let s = CStr::from_ptr(out_ptr).to_str().unwrap().to_owned();
            stryke_free_cstring(out_ptr as *mut c_char);
            s
        };
        serde_json::from_str(&parsed).unwrap()
    }

    #[test]
    fn geo_bounding_box_builds_box_clause() {
        let out = call_ffi(
            search__geo_bounding_box_query,
            json!({
                "field": "loc",
                "top_left": {"lat": 40.73, "lon": -74.1},
                "bottom_right": {"lat": 40.01, "lon": -71.12},
            }),
        );
        let bb = &out["value"]["geo_bounding_box"]["loc"];
        assert_eq!(bb["top_left"]["lat"], 40.73);
        assert_eq!(bb["bottom_right"]["lon"], -71.12);
    }

    #[test]
    fn geo_bounding_box_missing_field_errors() {
        let out = call_ffi(search__geo_bounding_box_query, json!({}));
        assert!(
            out.get("error").is_some(),
            "missing field must error: {out}"
        );
    }

    #[test]
    fn function_score_carries_query_functions_and_modes() {
        let out = call_ffi(
            search__function_score_query,
            json!({
                "query": {"match_all": {}},
                "functions": [{"random_score": {}}],
                "score_mode": "sum",
                "boost_mode": "multiply",
            }),
        );
        let fs = &out["value"]["function_score"];
        assert!(fs["query"]["match_all"].is_object());
        assert_eq!(fs["functions"][0]["random_score"], json!({}));
        assert_eq!(fs["score_mode"], "sum");
        assert_eq!(fs["boost_mode"], "multiply");
        // optional knobs absent → not emitted
        assert!(fs.get("min_score").is_none());
    }

    #[test]
    fn more_like_this_carries_fields_like_and_tuning() {
        let out = call_ffi(
            search__more_like_this_query,
            json!({
                "fields": ["title", "body"],
                "like": "a quick brown fox",
                "min_term_freq": 1,
                "max_query_terms": 12,
            }),
        );
        let mlt = &out["value"]["more_like_this"];
        assert_eq!(mlt["fields"], json!(["title", "body"]));
        assert_eq!(mlt["like"], "a quick brown fox");
        assert_eq!(mlt["min_term_freq"], 1);
        assert_eq!(mlt["max_query_terms"], 12);
        assert!(mlt.get("min_doc_freq").is_none());
    }

    #[test]
    fn agg_top_hits_passes_only_present_knobs() {
        let out = call_ffi(
            search__agg_top_hits,
            json!({"size": 3, "sort": [{"ts": {"order": "desc"}}]}),
        );
        let th = &out["value"]["top_hits"];
        assert_eq!(th["size"], 3);
        assert_eq!(th["sort"][0]["ts"]["order"], "desc");
        assert!(th.get("_source").is_none());
        assert!(th.get("from").is_none());
    }

    #[test]
    fn agg_global_is_empty_object() {
        let out = call_ffi(search__agg_global, json!({}));
        assert_eq!(out["value"]["global"], json!({}));
    }
}
