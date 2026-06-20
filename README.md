```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                  [ s e a r c h ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-search/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-search/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[ELASTICSEARCH / OPENSEARCH CLIENT FOR STRYKE // INDEX + DOC CRUD + BULK + QUERY DSL + SCROLL]`

> *"Full-text search, one stryke pipe at a time."*

Elasticsearch / OpenSearch client for stryke. Index administration, document
CRUD, bulk indexing, the query DSL, scroll, aliases, and cluster health against
any Elasticsearch 7+/8+ or OpenSearch 1+/2+ cluster. Both engines speak the same
REST API, so one client covers both. Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-mongo`](https://github.com/MenkeTechnologies/stryke-mongo) · [`stryke-postgres`](https://github.com/MenkeTechnologies/stryke-postgres) · [`stryke-redis`](https://github.com/MenkeTechnologies/stryke-redis)

---

## Table of Contents

- [\[0x00\] Install](#0x00-install)
- [\[0x01\] Quick start](#0x01-quick-start)
- [\[0x02\] Connecting](#0x02-connecting)
- [\[0x03\] Architecture](#0x03-architecture)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] Build & test](#0x05-build--test)
- [\[0x06\] License](#0x06-license)

---

## \[0x00\] Install

```sh
s add github.com/MenkeTechnologies/stryke-search
```

This records the dependency in your project's `stryke.toml` and downloads the
prebuilt cdylib for your host triple from the GitHub release (SHA-256 verified).
On first `use Search`, stryke dlopens the cdylib in-process and registers every
`search__*` export.

---

## \[0x01\] Quick start

```perl
use Search

var %conn
$conn{url} = "http://127.0.0.1:9200"

# create an index with a mapping
Search::index_create(
    "books",
    body => { mappings => { properties => { title => { type => "text" }, year => { type => "integer" } } } },
    %conn,
)

# index a document and make it searchable
Search::doc_index("books", { title => "rust in action", year => 2019 }, id => "1", %conn)
Search::index_refresh(index => "books", %conn)

# search it
val $res = Search::search("books", Search::match("title", "rust"), %conn)
p $res->{hits}{total}{value}      # 1

# count with a range query
val $cnt = Search::count("books", Search::range("year", gte => 2019), %conn)
p $cnt->{count}                   # 1
```

---

## \[0x02\] Connecting

Connection params come from the `%conn` opts hash on every call (or `$SEARCH_URL`
as a fallback when neither `url` nor `host` is given):

| Key        | Default       | Notes                                              |
| ---------- | ------------- | -------------------------------------------------- |
| `url`      | —             | Full base URL, e.g. `https://es.example.com:9243`  |
| `host`     | `127.0.0.1`   | Used with `port`/`tls` when `url` is absent        |
| `port`     | `9200`        |                                                    |
| `tls`      | `false`       | `true` selects the `https` scheme                  |
| `username` | —             | HTTP Basic auth                                    |
| `password` | —             | HTTP Basic auth                                    |
| `api_key`  | —             | Sent as `Authorization: ApiKey <key>` (wins over Basic) |
| `params`   | —             | Hash of extra URL query params per request         |

A `ureq::Agent` (HTTP keep-alive pool) is cached per `(base_url, auth)` for the
life of the stryke process, so repeated calls reuse pooled sockets.

---

## \[0x03\] Architecture

- **Transport** — the cluster REST API over [`ureq`](https://docs.rs/ureq):
  synchronous, pure-Rust, rustls-backed. No tokio, no OpenSSL.
- **One client, two engines** — Elasticsearch and OpenSearch expose the same
  `_search` / `_bulk` / `_doc` / `_cat` / `_cluster` endpoints.
- **JSON-in / JSON-out FFI** — each `search__*` export takes a JSON args dict and
  returns JSON; handlers run inside `catch_unwind` so a panic becomes an error
  result, never an unwind across the FFI boundary.
- **Pure builders** — the query-DSL builders and URL helpers take no connection
  and are unit-tested in-crate, so they validate in CI with no cluster.

---

## \[0x04\] API reference

The FFI surface, grouped:

| Group              | Functions                                                                                                   |
| ------------------ | ----------------------------------------------------------------------------------------------------------- |
| Cluster + nodes    | `version`, `ping`, `info`, `health`, `cat`, `raw`, `cluster_stats`, `cluster_state`, `cluster_settings_get`, `cluster_settings_put`, `nodes_info`, `nodes_stats`, `pending_tasks`, `allocation_explain` |
| Index admin        | `index_create`, `index_delete`, `index_exists`, `index_list`, `index_refresh`, `index_open`, `index_close`, `index_stats`, `settings_get`, `settings_update`, `mapping_get`, `mapping_put`, `field_caps` |
| Aliases            | `alias_add`, `alias_remove`, `alias_get`                                                                     |
| Templates          | `index_template_put`, `index_template_get`, `index_template_delete`, `index_template_exists`, `component_template_put`, `component_template_get`, `component_template_delete` |
| Documents          | `doc_index`, `doc_get`, `doc_exists`, `doc_update`, `doc_delete`, `mget`, `bulk`, `termvectors`, `mtermvectors` |
| Search             | `search`, `count`, `msearch`, `search_aggs`, `scroll_start`, `scroll_next`, `scroll_clear`, `delete_by_query`, `update_by_query`, `reindex`, `analyze`, `explain`, `pit_open`, `pit_close` |
| Query DSL builders | `match_all`, `match`, `match_phrase`, `match_phrase_prefix`, `term`, `terms`, `range`, `prefix`, `wildcard`, `regexp`, `fuzzy`, `exists`, `ids`, `query_string`, `simple_query_string`, `multi_match`, `geo_distance`, `nested`, `constant_score`, `dis_max`, `bool` |
| Body + fields      | `query_body`, `sort`, `highlight`, `bulk_ndjson`, `escape`                                                   |
| Aggregations       | `agg`, `agg_terms`, `agg_avg`, `agg_sum`, `agg_min`, `agg_max`, `agg_stats`, `agg_extended_stats`, `agg_cardinality`, `agg_value_count`, `agg_percentiles`, `agg_histogram`, `agg_date_histogram`, `agg_range`, `agg_filter`, `agg_missing`, `agg_nested` |
| Ingest pipelines   | `ingest_put`, `ingest_get`, `ingest_delete`, `ingest_simulate`                                               |
| Snapshot + repo    | `repo_create`, `repo_get`, `repo_delete`, `snapshot_create`, `snapshot_get`, `snapshot_delete`, `snapshot_restore` |
| Tasks              | `tasks_list`, `tasks_get`, `tasks_cancel`                                                                    |
| Stored scripts     | `script_put`, `script_get`, `script_delete`, `search_template`, `render_template`                           |
| URL helpers        | `build_url`, `parse_url`, `redact_url`                                                                       |

Query builders return **bare clauses** (e.g. `{"match":{…}}`), so they nest
directly inside `bool` / `nested` / `constant_score`. `Search::search` and
`::count` auto-wrap a top-level clause as `{"query": clause}`; pass a full body
(built with `Search::query_body`) when you need `aggs` / `sort` / `size`.

```perl
# bool query: must match title, filter by year range
val $q = Search::bool(
    must   => [ Search::match("title", "rust") ],
    filter => [ Search::range("year", gte => 2018) ],
)
val $hits = Search::search("books", $q, %conn)

# aggregations: average price bucketed by category
val $res = Search::search_aggs(
    "products",
    { by_cat => Search::agg_terms("category", size => 10) },
    %conn,
)
p $res->{aggregations}{by_cat}{buckets}

# full body with query + sort + size
val $body = Search::query_body(
    query => Search::match("title", "rust"),
    sort  => Search::sort("year", order => "desc"),
    size  => 20,
)
val $page = Search::search("books", $body, %conn)
```

Bulk ops are an array of op hashes; the cdylib serializes the NDJSON body:

```perl
Search::bulk(
    [
        { action => "index",  id => "2", document => { title => "the rust book" } },
        { action => "update", id => "1", doc      => { year => 2020 } },
        { action => "delete", id => "3" },
    ],
    index => "books",
    %conn,
)
```

---

## \[0x05\] Build & test

```sh
make debug       # cargo build
make test        # cargo test, then `s test t/` (needs $SEARCH_URL or 127.0.0.1:9200)
make install     # s pkg install -g . (cdylib lands in ~/.stryke/store/search@<ver>/)
```

`cargo test` runs the in-crate unit tests (NDJSON build, Lucene escaping, URL
parse/redact, basic-auth header, percent-encoding) with no cluster required.
The `t/test_stryke_search_surface.stk` pins the wrapper surface and the query-DSL
builders; `t/test_search.stk` runs end-to-end CRUD + search against a live
cluster and short-circuits when none answers.

---

## \[0x06\] License

MIT &middot; MenkeTechnologies
