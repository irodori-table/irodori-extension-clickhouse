<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# ClickHouse Connector

Native Irodori Table connector extension for ClickHouse.

This crate packages the connector metadata, native ABI exports, and driver implementation used by the Irodori extension marketplace.

## Connector

- Extension ID: `irodori.clickhouse`
- Engine ID: `clickhouse`
- Wire protocol: `clickhouse`
- Default port: `8123`
- Native ABI: `irodori.connector.native.v1`
- Driver linked: `yes`
- Marketplace visibility: `public`
- Package version: `0.1.8`

The package includes a desktop adapter source snapshot from `db/clickhouse.rs`.

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust crate exports the native ABI from `src/lib.rs`, uses `irodori-connector-abi` for shared JSON/buffer helpers, and keeps connector behavior in `src/driver.rs`.

## Connection Metadata

- Endpoint modes: `hostPort`, `connectionString`
- Transport modes: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS supported: `yes`
- TLS required by default: `no`
- Custom driver options: `yes`

### Endpoint Fields

| Field | Label | Type | Required |
| --- | --- | --- | --- |
| `host` | Host | `string` | yes |
| `protocol` | Protocol (http, native, mysql, postgresql) | `string` | yes |
| `secure` | Use TLS | `boolean` | no |
| `port` | Port | `number` | no |
| `database` | Database | `string` | no |

## Authentication

The connector advertises these authentication modes so clients can render the right credential fields. Driver-specific or provider-specific values can still be passed through `options` when needed.

| Auth method | Label | Kind | Secret purposes |
| --- | --- | --- | --- |
| `none` | No authentication | `none` | none |
| `connectionString` | Connection string / DSN | `connectionString` | none |
| `userPassword` | User/password | `userPassword` | `password` |
| `bearerToken` | Bearer token | `token` | `token` |
| `clientCertificate` | Client certificate / mTLS | `certificate` | `privateKey`, `privateKeyPassphrase` |
| `customDriverOptions` | Custom driver options | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## Experience Metadata

- Domains: `timeSeries`
- Result views: `timeChart`, `table`, `heatmap`
- Object types: `tables`, `columns`, `partitions`, `projections`, `materializedViews`
- Inspired by: ClickHouse SQL console, time bucketing, latest-point analytics

| Workflow | Result view | Templates |
| --- | --- | --- |
| Bucketed aggregate | `timeChart` | `time-clickhouse-bucket` |
| Latest event per key | `table` | `time-clickhouse-latest` |

| Template | Label | Language | Result view |
| --- | --- | --- | --- |
| `time-clickhouse-bucket` | Bucketed aggregate | `sql` | `timeChart` |
| `time-clickhouse-latest` | Latest per key | `sql` | `table` |

## Native ABI Calls

| Method | Response |
| --- | --- |
| `health` | Returns connector health, engine id, ABI version, and driver status. |
| `describe` | Returns the embedded manifest and connector config. |
| `manifest` | Returns raw `irodori.extension.json`. |
| `config` | Returns raw `connector.config.json`. |
| `connect` | Opens and validates a native connector connection. |
| `query` | Runs a connector query and returns structured rows or JSON results. |
| `metadata` | Reads schemas, tables, columns, indexes, collections, or equivalent metadata. |
| `close` | Closes and removes a cached native connection. |

## Development

All extension crates in this checkout share `../target` so dependencies compile once across sibling repositories.

```sh
make check
make build
```

Release packages place platform-specific native artifacts under `dist/native`.

## License

0BSD. You can use, copy, modify, and distribute this project for almost any purpose.
