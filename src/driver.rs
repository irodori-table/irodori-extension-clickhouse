use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use reqwest::{Client, RequestBuilder};
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, ClickHouseConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct ClickHouseConnection {
    client: Client,
    config: ClickHouseConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClickHouseConfig {
    base_url: String,
    database: String,
    username: Option<String>,
    password: Option<String>,
    bearer_token: Option<String>,
    tls: TlsConfig,
    redaction_values: Vec<String>,
}

/// Transport security, as `connector.config.json` declares it under
/// `clientCertificate`.
///
/// Paths, never key material: connector options persist to the workspace in the
/// clear, so the profile carries a path and the driver reads the file at
/// connect time.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TlsConfig {
    root_cert_path: Option<String>,
    client_cert_path: Option<String>,
    client_key_path: Option<String>,
    accept_invalid_certs: bool,
}

#[derive(Default)]
struct ObjectMeta {
    name: String,
    columns: Vec<Value>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, ClickHouseConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match ClickHouseConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let client = match config.tls.build_client() {
        Ok(client) => client,
        Err(err) => return abi::error("connector.invalidRequest", config.redact(&err)),
    };
    let connection = ClickHouseConnection { client, config };
    let version = match runtime().and_then(|runtime| runtime.block_on(load_version(&connection))) {
        Ok(version) => version,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "endpoint".to_string(),
            Value::String(connection.config.base_url.clone()),
        ),
        (
            "database".to_string(),
            Value::String(connection.config.database.clone()),
        ),
        ("serverVersion".to_string(), Value::String(version)),
    ]);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime()
        .and_then(|runtime| runtime.block_on(run_query(&connection, sql, abi::max_rows(request))))
    {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl ClickHouseConnection {
    fn auth(&self, builder: RequestBuilder) -> RequestBuilder {
        if let Some(token) = self.config.bearer_token.as_deref() {
            builder.bearer_auth(token)
        } else if let Some(username) = self.config.username.as_deref() {
            builder.basic_auth(username, self.config.password.as_deref())
        } else {
            builder
        }
    }
}

impl TlsConfig {
    /// A client honouring the profile's TLS material.
    ///
    /// Returns the default client untouched when nothing is configured, so a
    /// plain `http://` ClickHouse keeps working exactly as before.
    fn build_client(&self) -> Result<Client, String> {
        if self.root_cert_path.is_none()
            && self.client_cert_path.is_none()
            && self.client_key_path.is_none()
            && !self.accept_invalid_certs
        {
            return Client::builder()
                .build()
                .map_err(|err| format!("HTTP client setup failed: {err}"));
        }

        let mut builder = Client::builder();

        if let Some(path) = &self.root_cert_path {
            let pem = read_pem(path, "SSL root certificate")?;
            let bundle = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|err| format!("SSL root certificate at {path} is not valid PEM: {err}"))?;
            // `from_pem_bundle` answers Ok(vec![]) for a file with no PEM
            // blocks in it. Adding nothing and carrying on would fall back to
            // the system roots — the connection would succeed while verifying
            // against something other than the CA the user named.
            if bundle.is_empty() {
                return Err(format!(
                    "SSL root certificate at {path} contains no PEM certificate."
                ));
            }
            for certificate in bundle {
                builder = builder.add_root_certificate(certificate);
            }
        }

        // reqwest wants one PEM carrying both halves. Accept them as separate
        // files, which is how every other tool spells it, and join them here.
        match (&self.client_cert_path, &self.client_key_path) {
            (Some(cert_path), Some(key_path)) => {
                let mut pem = read_pem(cert_path, "SSL client certificate")?;
                if !pem.ends_with(b"\n") {
                    pem.push(b'\n');
                }
                pem.extend_from_slice(&read_pem(key_path, "SSL client key")?);
                builder = builder.identity(
                    reqwest::Identity::from_pem(&pem)
                        .map_err(|err| format!("SSL client identity is not usable: {err}"))?,
                );
            }
            (Some(_), None) => {
                return Err("SSL client certificate needs a matching client key.".to_string())
            }
            (None, Some(_)) => {
                return Err("SSL client key needs a matching client certificate.".to_string())
            }
            (None, None) => {}
        }

        if self.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }

        builder
            .build()
            .map_err(|err| format!("TLS client setup failed: {err}"))
    }
}

fn read_pem(path: &str, label: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|err| format!("{label} at {path} could not be read: {err}"))
}

impl ClickHouseConfig {
    fn from_request(request: &Value) -> Result<Self, String> {
        let base_url = option_string(request, &["connectionString", "url", "dsn"])
            .unwrap_or_else(|| build_url(request));
        let database =
            option_string(request, &["database", "db"]).unwrap_or_else(|| "default".into());
        let username = option_string(request, &["user", "username"]);
        let password = option_string(request, &["password"]);
        let bearer_token = option_string(request, &["token", "bearerToken", "accessToken"]);
        let tls = TlsConfig {
            root_cert_path: option_string(
                request,
                &["sslRootCert", "sslrootcert", "ssl-ca", "caCert"],
            ),
            client_cert_path: option_string(
                request,
                &["sslCert", "sslcert", "ssl-cert", "clientCert"],
            ),
            client_key_path: option_string(request, &["sslKey", "sslkey", "ssl-key", "clientKey"]),
            accept_invalid_certs: option_string(request, &["sslInsecure", "tlsInsecure"])
                .is_some_and(|value| matches!(value.trim(), "1" | "true" | "yes")),
        };
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, password.as_deref());
        push_sensitive(&mut redaction_values, bearer_token.as_deref());
        collect_url_auth(&base_url, &mut redaction_values);
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            database,
            username,
            password,
            bearer_token,
            tls,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values.iter().fold(
            message.replace(&self.base_url, "<clickhouse-url>"),
            |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            },
        )
    }
}

async fn load_version(connection: &ClickHouseConnection) -> Result<String, String> {
    let (_, rows, _) = run_query(connection, "SELECT version()", 1).await?;
    Ok(rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_str)
        .map(|version| format!("ClickHouse {version}"))
        .unwrap_or_else(|| "ClickHouse".to_string()))
}

async fn run_query(
    connection: &ClickHouseConnection,
    sql: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let url = format!(
        "{}/?database={}&default_format=JSON",
        connection.config.base_url,
        url_component(&connection.config.database)
    );
    let response = connection
        .auth(connection.client.post(url))
        .body(sql.to_string())
        .send()
        .await
        .map_err(|err| format!("ClickHouse HTTP request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("ClickHouse response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("ClickHouse query returned HTTP {status}: {text}"));
    }
    if text.trim().is_empty() {
        return Ok((Vec::new(), Vec::new(), false));
    }
    let value = serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("ClickHouse JSON response parse failed: {err}: {text}"))?;
    Ok(clickhouse_response_to_output(value, cap))
}

async fn load_metadata(connection: &ClickHouseConnection) -> Result<Value, String> {
    let sql = format!(
        "SELECT table, name, type, position FROM system.columns \
         WHERE database = '{}' ORDER BY table, position",
        connection.config.database.replace('\'', "''")
    );
    let (columns, rows, _) = run_query(connection, &sql, 10_000).await?;
    Ok(metadata_from_columns(
        &connection.config.database,
        &columns,
        rows,
    ))
}

fn clickhouse_response_to_output(value: Value, cap: usize) -> QueryOutput {
    let columns = value
        .get("meta")
        .and_then(Value::as_array)
        .map(|meta| {
            meta.iter()
                .filter_map(|column| column.get("name").and_then(Value::as_str))
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut rows = Vec::new();
    let mut truncated = false;
    for row in data {
        if rows.len() >= cap {
            truncated = true;
            break;
        }
        rows.push(if let Some(object) = row.as_object() {
            columns
                .iter()
                .map(|column| object.get(column).cloned().unwrap_or(Value::Null))
                .collect()
        } else {
            vec![row]
        });
    }
    (columns, rows, truncated)
}

fn metadata_from_columns(database: &str, columns: &[String], rows: QueryRows) -> Value {
    let table_idx = columns.iter().position(|column| column == "table");
    let name_idx = columns.iter().position(|column| column == "name");
    let type_idx = columns.iter().position(|column| column == "type");
    let position_idx = columns.iter().position(|column| column == "position");
    let mut objects: BTreeMap<String, ObjectMeta> = BTreeMap::new();
    let (Some(table_idx), Some(name_idx), Some(type_idx)) = (table_idx, name_idx, type_idx) else {
        return json!({ "schemas": [{ "name": database, "objects": [] }] });
    };
    for row in rows {
        let table = string_cell(&row, table_idx);
        let name = string_cell(&row, name_idx);
        if table.is_empty() || name.is_empty() {
            continue;
        }
        let data_type = string_cell(&row, type_idx);
        let ordinal = position_idx
            .and_then(|idx| row.get(idx))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let object = objects.entry(table.clone()).or_insert_with(|| ObjectMeta {
            name: table,
            columns: Vec::new(),
        });
        object.columns.push(json!({
            "name": name,
            "dataType": data_type,
            "nullable": true,
            "ordinal": ordinal
        }));
    }
    json!({
        "schemas": [{
            "name": database,
            "objects": objects
                .into_values()
                .map(|object| {
                    json!({
                        "schema": database,
                        "name": object.name,
                        "kind": "table",
                        "columns": object.columns,
                        "indexes": [],
                        "primaryKey": [],
                        "foreignKeys": []
                    })
                })
                .collect::<Vec<_>>()
        }]
    })
}

fn string_cell(row: &[Value], index: usize) -> String {
    row.get(index)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn build_url(request: &Value) -> String {
    let host = option_string(request, &["host", "endpoint"]).unwrap_or_else(|| "127.0.0.1".into());
    let port = option_string(request, &["port"]).unwrap_or_else(|| "8123".into());
    let scheme = if bool_option(request, &["tls", "ssl"]).unwrap_or(false) {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{host}:{port}")
}

fn connection(connection_id: &str) -> Result<ClickHouseConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .map(|value| match value {
                        Value::String(value) => value.clone(),
                        Value::Number(value) => value.to_string(),
                        Value::Bool(value) => value.to_string(),
                        _ => String::new(),
                    })
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn bool_option(request: &Value, fields: &[&str]) -> Option<bool> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields
                .iter()
                .find_map(|field| container.get(*field).and_then(Value::as_bool))
        })
}

fn url_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }
}

fn collect_url_auth(url: &str, values: &mut Vec<String>) {
    let Some(after_scheme) = url.split_once("://").map(|(_, rest)| rest) else {
        return;
    };
    let Some(auth) = after_scheme
        .split('/')
        .next()
        .and_then(|host| host.split('@').next())
    else {
        return;
    };
    if auth.contains(':') {
        for part in auth.split(':') {
            push_sensitive(values, Some(part));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_clickhouse_json_response() {
        let value = json!({
            "meta": [{"name": "name"}, {"name": "count"}],
            "data": [{"name": "a", "count": 2}]
        });
        let (columns, rows, truncated) = clickhouse_response_to_output(value, 10);
        assert_eq!(columns, vec!["name", "count"]);
        assert_eq!(rows, vec![vec![json!("a"), json!(2)]]);
        assert!(!truncated);
    }

    #[test]
    fn builds_metadata_from_system_columns() {
        let columns = vec![
            "table".to_string(),
            "name".to_string(),
            "type".to_string(),
            "position".to_string(),
        ];
        let rows = vec![vec![
            json!("events"),
            json!("ts"),
            json!("DateTime"),
            json!(1),
        ]];
        let metadata = metadata_from_columns("default", &columns, rows);
        assert_eq!(metadata["schemas"][0]["objects"][0]["name"], "events");
        assert_eq!(
            metadata["schemas"][0]["objects"][0]["columns"][0]["name"],
            "ts"
        );
    }

    #[test]
    fn builds_url_from_profile() {
        let request = json!({"profile": {"host": "click.local", "port": 8443, "tls": true}});
        let config = ClickHouseConfig::from_request(&request).unwrap();
        assert_eq!(config.base_url, "https://click.local:8443");
        assert_eq!(config.database, "default");
    }

    #[test]
    fn reads_tls_paths_from_the_connector_options() {
        let config = ClickHouseConfig::from_request(&json!({
            "profile": {
                "host": "clickhouse.local",
                "options": {
                    "sslRootCert": "/etc/ssl/ca.pem",
                    "sslCert": "/etc/ssl/client.pem",
                    "sslKey": "/etc/ssl/client.key"
                }
            }
        }))
        .unwrap();
        assert_eq!(
            config.tls.root_cert_path.as_deref(),
            Some("/etc/ssl/ca.pem")
        );
        assert_eq!(
            config.tls.client_cert_path.as_deref(),
            Some("/etc/ssl/client.pem")
        );
        assert_eq!(
            config.tls.client_key_path.as_deref(),
            Some("/etc/ssl/client.key")
        );
        assert!(!config.tls.accept_invalid_certs);
    }

    #[test]
    fn accepts_the_driver_spellings_of_the_tls_options() {
        let config = ClickHouseConfig::from_request(&json!({
            "profile": { "options": { "sslrootcert": "/ca.pem", "ssl-cert": "/c.pem" } }
        }))
        .unwrap();
        assert_eq!(config.tls.root_cert_path.as_deref(), Some("/ca.pem"));
        assert_eq!(config.tls.client_cert_path.as_deref(), Some("/c.pem"));
    }

    #[test]
    fn a_profile_without_tls_options_keeps_the_plain_client() {
        let config = ClickHouseConfig::from_request(&json!({
            "profile": { "host": "clickhouse.local" }
        }))
        .unwrap();
        assert_eq!(config.tls, TlsConfig::default());
        assert!(config.tls.build_client().is_ok());
    }

    #[test]
    fn half_a_client_identity_is_rejected_with_a_usable_message() {
        // Silently ignoring the half that was supplied would connect without
        // the certificate the user asked for.
        let cert_only = TlsConfig {
            client_cert_path: Some("/etc/ssl/client.pem".into()),
            ..TlsConfig::default()
        };
        assert_eq!(
            cert_only.build_client().unwrap_err(),
            "SSL client certificate needs a matching client key."
        );

        let key_only = TlsConfig {
            client_key_path: Some("/etc/ssl/client.key".into()),
            ..TlsConfig::default()
        };
        assert_eq!(
            key_only.build_client().unwrap_err(),
            "SSL client key needs a matching client certificate."
        );
    }

    #[test]
    fn an_unreadable_certificate_names_the_file_and_the_field() {
        let config = TlsConfig {
            root_cert_path: Some("/definitely/not/here.pem".into()),
            ..TlsConfig::default()
        };
        let err = config.build_client().unwrap_err();
        assert!(
            err.starts_with("SSL root certificate at /definitely/not/here.pem"),
            "{err}"
        );
    }

    #[test]
    fn a_certificate_that_is_not_pem_is_rejected() {
        let dir = std::env::temp_dir().join("irodori-clickhouse-tls-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("not-a-cert.pem");
        std::fs::write(&path, b"this is not a certificate").unwrap();

        let config = TlsConfig {
            root_cert_path: Some(path.to_string_lossy().into_owned()),
            ..TlsConfig::default()
        };
        // reqwest answers Ok(vec![]) rather than an error for a file with no
        // PEM blocks, so without an explicit emptiness check this connection
        // would silently verify against the system roots instead of the named
        // CA.
        let err = config.build_client().unwrap_err();
        assert!(err.contains("contains no PEM certificate"), "{err}");

        std::fs::remove_file(&path).ok();
    }
}
