# Setup Guide

Step-by-step instructions for getting this POC running from a clean checkout,
including exactly where to plug in your own database credentials, Kafka
broker address and TLS certificates. For a high-level explanation of what the
project does, see `README.md`. For endpoint-by-endpoint usage, see
`API_DOCUMENTATION.md`. For the end-to-end data flow and diagrams, see
`ARCHITECTURE.md`.

## 1. Prerequisites

| Tool | Version used in this POC | Notes |
|---|---|---|
| JDK | 11+ (tested on JDK 21 runtime, `<release>11</release>` bytecode target) | `java -version` |
| Maven | 3.8+ | Or use the wrapper `./mvnw` if `.mvn/wrapper` is present |
| Apache Kafka | 3.x | Local single-broker cluster is enough for a demo |
| MySQL | 8.0+ | Sink database for the Flink `KafkaItemToMysqlJob` |
| SQL Server (optional) | any recent version | Only needed if you want to exercise the real `MssqlItemToKafkaJob` source read; otherwise point `ITEM_MSSQL_URL` at a MySQL/H2 substitute or skip Job 1 |
| Apache Flink | 1.20.x | Only required if running the Flink jobs outside the embedded `StreamExecutionEnvironment.getExecutionEnvironment()` (local execution mode works without a separate cluster) |

## 2. Clone and build

```powershell
git clone <your-fork-url> item-kafka-producer-poc
cd item-kafka-producer-poc
mvn -q compile
```

## 3. Configuration model: environment variable -> YAML property -> Java class

Nothing sensitive is hardcoded. Every secret/connection value has an
`${ENV_VAR:default}` placeholder in `application.yml` (and the
`application_dev.yml` / `_int.yml` / `_qa.yml` profile overlays). Set the
environment variable before starting the app to override the default.

| Environment variable | YAML property | Bound to Java class | Used for |
|---|---|---|---|
| `ITEM_KAFKA_BOOTSTRAP_SERVERS` | `spring.kafka.bootstrap-servers` | `KafkaProperties` | Kafka broker address(es) for the producer, consumer and both Flink jobs |
| `ITEM_KAFKA_TOPIC` | `spring.kafka.item-topic-name` | `KafkaProperties` | Shared Kafka topic name (default `Item_Topic`) |
| `ITEM_MSSQL_URL` | `spring.datasource.url` | `MSSQLDataSourceProperties` (and Spring `DataSource` autoconfig) | Source SQL Server JDBC connection string read by `ItemRepository`/JPA and by `MssqlItemToKafkaJob`. Can use `integratedSecurity=true` for Windows Integrated Authentication - see `DATABASE_SETUP.md` section 1.1 for exactly how that works and what it requires |
| `ITEM_MSSQL_SOURCE_TABLE` | `spring.datasource.source-table-name` | `MSSQLDataSourceProperties` | Source table name for `MssqlItemToKafkaJob` (default `ITEM`) |
| `ITEM_MYSQL_URL` | `spring.mysql.jdbcUrl` | `MySqlProperties` | MySQL JDBC URL for the Flink `KafkaItemToMysqlJob` sink |
| `ITEM_MYSQL_USERNAME` | `spring.mysql.username` | `MySqlProperties` | MySQL username |
| `ITEM_MYSQL_PASSWORD` | `spring.mysql.password` | `MySqlProperties` | MySQL password |
| `ITEM_MYSQL_TABLE` | `spring.mysql.item-table-name` | `MySqlProperties` | Destination table name for `KafkaItemToMysqlJob` (default `ITEM`) |
| `ITEM_SSL_KEYSTORE` | `keys.ssl.keyStore` | `SSLProperties` | Path to the client keystore (`.jks`/`.p12`) used for the optional mutual-TLS gateway call in `MsgRoutingServiceImpl#getGatewaySSLTemplateConfig` |
| `ITEM_SSL_KEYSTORE_PASSWORD` | `keys.ssl.keyStorePassword` | `SSLProperties` | Password protecting that keystore |
| `ITEM_SSL_TRUSTSTORE` | `keys.ssl.trustStore` | `SSLProperties` | Path to the truststore containing trusted CA certs |
| `ITEM_SSL_TRUSTSTORE_PASSWORD` | `keys.ssl.trustStorePassword` | `SSLProperties` | Password protecting that truststore |
| `ITEM_JWT_PRIVATE_KEY` | `jwt.private-key` | `JwtTokenUtil` | RSA private key (PEM, PKCS#1 or PKCS#8) used to sign the gateway JWT in `MsgConsumerController` / `MsgRoutingServiceImpl` |
| `ITEM_JWT_ISSUER` | `jwt.issuer` | `JwtTokenUtil` | JWT `iss` claim (default `item-kafka-producer`) |
| `ITEM_JWT_EXPIRY_MINUTES` | `jwt.expiry-minutes` | `JwtTokenUtil` | JWT expiry window in minutes (default `30`) |
| `ITEM_GATEWAY_URL` | `gateway.endpoint.url` | `MsgRoutingServiceImpl` | Downstream HTTP endpoint that `MsgConsumerController`'s send/consume test flow forwards to |
| `SYSLOG_HOST` | `syslog.logging.host` | `logback-spring.xml` | Optional remote syslog host for log shipping |

None of these need to be set to run the basic producer/consumer demo against
a local Kafka broker only - they all have safe local-dev defaults. You only
need to fill in real values for the pieces you intend to exercise.

### 3.1 Setting environment variables locally (PowerShell)

```powershell
$env:ITEM_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:ITEM_MYSQL_URL      = "jdbc:mysql://localhost:3306/item_poc?useSSL=false&allowPublicKeyRetrieval=true"
$env:ITEM_MYSQL_USERNAME = "item_poc_user"
$env:ITEM_MYSQL_PASSWORD = "<your-mysql-password>"
$env:ITEM_MSSQL_URL      = "jdbc:sqlserver://<your-host>:1433;databaseName=<your-db>;encrypt=false"
```

For anything you don't set, the `:default` value after the colon in
`application.yml` is used instead.

## 4. Bringing your own TLS keystore/truststore (optional path)

The mutual-TLS gateway call in `MsgRoutingServiceImpl` is only exercised if
you invoke `getGatewaySSLTemplateConfig()` against a gateway that requires a
client certificate. If you don't need that flow, leave `keys.ssl.enabled`
`false` and skip this section entirely.

If you do want to demo it:

1. Generate (or obtain) a PKCS#12/JKS keystore containing your client
   certificate and private key, e.g.:
   ```powershell
   keytool -genkeypair -alias item-poc-client -keyalg RSA -keysize 2048 `
     -validity 365 -keystore item-poc-client.jks -storetype JKS
   ```
2. Copy the resulting file somewhere on disk (never commit it to Git).
3. Point the environment variables at it:
   ```powershell
   $env:ITEM_SSL_KEYSTORE = "C:\certs\item-poc-client.jks"
   $env:ITEM_SSL_KEYSTORE_PASSWORD = "<the password you chose above>"
   $env:ITEM_SSL_TRUSTSTORE = "C:\certs\cacerts"
   $env:ITEM_SSL_TRUSTSTORE_PASSWORD = "changeit"
   ```

## 5. Generating a JWT signing key (optional path)

Only needed to demo `MsgConsumerController`'s `send-items` endpoint, which
signs a gateway JWT via `JwtTokenUtil`.

```powershell
# Generates a PKCS#8 PEM private key (no passphrase) for demo purposes only.
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out item-poc-jwt-key.pem
```

Set the full PEM content (including `-----BEGIN PRIVATE KEY-----` /
`-----END PRIVATE KEY-----` lines) as `ITEM_JWT_PRIVATE_KEY`. In PowerShell,
one convenient way is to read the file into a single environment variable:

```powershell
$env:ITEM_JWT_PRIVATE_KEY = Get-Content .\item-poc-jwt-key.pem -Raw
```

`JwtTokenUtil` also accepts a bare PKCS#1 key body (no `BEGIN`/`END` headers);
it will wrap it automatically. Never commit `item-poc-jwt-key.pem` to Git -
it is already covered by `.gitignore` patterns for `*.pem`/`*.jks`/`*.p12` (add
one if it is not already present).

## 6. Database setup

Full DDL scripts for both MS SQL Server (source, including how Windows
Integrated Authentication works and how to grant your Windows account
access) and MySQL (source alternative + sink) are in **`DATABASE_SETUP.md`**.
Short version:

- **Source** (`ITEM` table read by `ItemRepository`/JPA and `MssqlItemToKafkaJob`): SQL Server DDL in `DATABASE_SETUP.md` section 1, or use the MySQL alternative in section 2 if you don't have SQL Server.
- **Sink** (`ITEM` table written by `KafkaItemToMysqlJob`): MySQL DDL in `DATABASE_SETUP.md` section 3.
- Fastest path if you just want *something* running: let Hibernate auto-create the source table (`spring.jpa.hibernate.ddl-auto: update`, already the default) against any reachable database, then seed a few rows manually.

## 7. Kafka setup and topic creation

Full install/configure/topic-creation walkthrough (download links, KRaft vs
ZooKeeper, Docker one-liner, verifying the broker, watching messages live) is
in **`KAFKA_SETUP.md`**. Minimum command once a broker is running:

```powershell
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic Item_Topic --partitions 3 --replication-factor 1
```
(use `kafka-topics.sh` on Linux/macOS)

## 8. Run the application

```powershell
mvn spring-boot:run "-Dspring-boot.run.profiles=dev"
```

The app starts on port `8082` by default (see `server.port`). Swagger UI is
available at `http://localhost:8082/agent/swagger-ui.html`.

## 9. Exercise the flows

See `API_DOCUMENTATION.md` for full curl examples for every endpoint,
including how to trigger `MssqlItemToKafkaJob`, `KafkaItemToMysqlJob` and the
`FlinkWordStreamDemoJob` smoke test via `FlinkJobController`.

