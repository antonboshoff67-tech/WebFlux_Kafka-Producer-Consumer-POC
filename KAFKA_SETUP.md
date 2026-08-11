# Kafka Setup

Instructions to install Apache Kafka locally, configure a single-broker
cluster suitable for this POC, and create the `Item_Topic` topic used by the
producer, consumer and both Flink jobs.

## 1. Download

- Official downloads: https://kafka.apache.org/downloads
- Pick the latest stable **binary** release, e.g. `kafka_2.13-3.8.0.tgz` (Scala 2.13 build, any recent 3.x release works).
- You do **not** need the source release, just the pre-built binary.

### Windows
```powershell
# Extract to a short path to avoid Windows MAX_PATH issues, e.g.:
Expand-Archive kafka_2.13-3.8.0.tgz -DestinationPath C:\kafka
```
(If it's a `.tgz`, use `tar -xzf kafka_2.13-3.8.0.tgz` from a shell that has `tar`, e.g. Git Bash, WSL, or PowerShell 5.1+ which ships `tar.exe`.)

### macOS/Linux
```bash
tar -xzf kafka_2.13-3.8.0.tgz
cd kafka_2.13-3.8.0
```

### Or via Docker (fastest path, no local install)
```powershell
docker run -d --name kafka -p 9092:9092 `
  -e KAFKA_NODE_ID=1 `
  -e KAFKA_PROCESS_ROLES=broker,controller `
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 `
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 `
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 `
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT `
  apache/kafka:3.8.0
```
This single container runs Kafka in KRaft mode (no separate ZooKeeper needed) and is enough for this POC. Skip straight to section 4 (create the topic) if you use this route.

### Or via the docker-compose file included in this repo (easiest)
A ready-to-use `docker-compose.kafka.yml` is included at the repository root:
```powershell
docker compose -f docker-compose.kafka.yml up -d
docker exec item-kafka-broker kafka-topics --bootstrap-server localhost:9092 --create --topic Item_Topic --partitions 1 --replication-factor 1
docker exec item-kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
docker exec item-kafka-broker kafka-topics --bootstrap-server localhost:9092 --describe --topic Item_Topic
```
Stop it later with `docker compose -f docker-compose.kafka.yml down` (add `-v` to also wipe the topic data).


## 2. Configure a local single-broker cluster (KRaft mode, no ZooKeeper)

Modern Kafka (3.x+) can run without ZooKeeper using KRaft. This is the simplest setup for a demo/portfolio project.

1. Generate a cluster ID and format the storage directory (run once):

   **Windows:**
   ```powershell
   cd C:\kafka\kafka_2.13-3.8.0
   $uuid = & .\bin\windows\kafka-storage.bat random-uuid
   .\bin\windows\kafka-storage.bat format -t $uuid -c .\config\kraft\server.properties
   ```

   **macOS/Linux:**
   ```bash
   cd kafka_2.13-3.8.0
   UUID=$(bin/kafka-storage.sh random-uuid)
   bin/kafka-storage.sh format -t "$UUID" -c config/kraft/server.properties
   ```

2. Review `config/kraft/server.properties` - the defaults are fine for local use. Key settings you may want to check:
   ```properties
   listeners=PLAINTEXT://:9092,CONTROLLER://:9093
   advertised.listeners=PLAINTEXT://localhost:9092
   log.dirs=/tmp/kraft-combined-logs
   num.partitions=1
   ```
   The `ITEM_KAFKA_BOOTSTRAP_SERVERS` environment variable used by this project should match `advertised.listeners` above, e.g. `localhost:9092`.

3. Start the broker:

   **Windows:**
   ```powershell
   .\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
   ```

   **macOS/Linux:**
   ```bash
   bin/kafka-server-start.sh config/kraft/server.properties
   ```

   Leave this running in its own terminal window while you use the app.

### 2.1 Older ZooKeeper-based setup (if you're on Kafka 2.x or prefer it)

```powershell
# Terminal 1: start ZooKeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Terminal 2: start the broker
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

## 3. Verify the broker is up

```powershell
.\bin\windows\kafka-broker-api-versions.bat --bootstrap-server localhost:9092
```
(or the `.sh` equivalent on macOS/Linux). Any output listing supported API versions confirms the broker is reachable.

## 4. Create the `Item_Topic` topic

This project expects a single shared topic (name configurable via
`ITEM_KAFKA_TOPIC`, default `Item_Topic`) used by:
- `ItemProducerService` (writes)
- `ItemConsumerService` (manual poll reads)
- `MssqlItemToKafkaJob` (writes)
- `KafkaItemToMysqlJob` (reads)

**Windows:**
```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 `
  --create --topic Item_Topic --partitions 3 --replication-factor 1
```

**macOS/Linux:**
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic Item_Topic --partitions 3 --replication-factor 1
```

Using more than 1 partition (e.g. 3) lets you see Flink/Kafka consumer
parallelism in action even on a single broker; 1 partition is also fine for a
minimal demo.

### 4.1 Confirm the topic exists

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic Item_Topic
```

Expected output shows the topic, its partition count, and `Leader`/`Replicas`/`Isr` all pointing at broker id `1`.

### 4.2 (Optional) Watch messages live while testing the app

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic Item_Topic --from-beginning
```

Leave this running in a spare terminal while you call
`POST /item-kafka/app/publish-items/v1` or `POST /flink/start-job1` - you
should see the JSON `Item` payloads scroll past in real time.

## 5. Point the app at your broker

```powershell
$env:ITEM_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:ITEM_KAFKA_TOPIC = "Item_Topic"
```

These map to `spring.kafka.bootstrap-servers` and
`spring.kafka.item-topic-name` respectively - see `SETUP_GUIDE.md` for the
full environment-variable reference table.

## 6. Tearing down

```powershell
# Stop the broker with Ctrl+C in its terminal, then optionally clear state:
Remove-Item -Recurse -Force C:\tmp\kraft-combined-logs
```

Or, if you used Docker: `docker rm -f kafka`.

