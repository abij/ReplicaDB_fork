<img src="https://img.shields.io/github/license/osalvador/replicadb?style=for-the-badge" alt="License"> <img src="https://img.shields.io/github/v/release/osalvador/replicadb?style=for-the-badge"  alt="Last Version">
<img src="https://img.shields.io/docker/pulls/osalvador/replicadb.svg?style=for-the-badge&logo=docker" alt="Docker Pull">
<img src="https://img.shields.io/github/downloads/osalvador/replicadb/total?style=for-the-badge&logo=github" alt="Github Downloads">
<img src="https://img.shields.io/github/stars/osalvador/replicadb.svg?style=for-the-badge&logo=github" alt="Github Start">
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4190/badge)](https://bestpractices.coreinfrastructure.org/projects/4190)

![replicadb-logo](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/replicadb-logo.png)

---

> ## Fork: [abij/ReplicaDB](https://github.com/abij/ReplicaDB) — fork of [osalvador/ReplicaDB](https://github.com/osalvador/ReplicaDB)
>
> **What this fork adds:**
> - **Azure Data Lake Storage Gen2 (ADLS Gen2) sink** — write Parquet, CSV, or ORC directly to ADLS Gen2 (`abfss://...`)
> - **Parquet output** — columnar Parquet files via Apache Parquet/Hadoop (replaces row-oriented CSV for analytics workloads)
> - **Sink stats file** — `--sink-stats-file` writes a JSON summary (rows, duration, file path per task) to a local file or ADLS Gen2
> - **DB2 partition column exclusion** — internal `RN` partition column is stripped from sink output
> - **Windows path support** — local file paths with drive letters (e.g. `C:\...`) are handled correctly alongside URI schemes
> - **Java 21 build, Java 11 runtime** — compiled with JDK 21 targeting Java 11 bytecode; all dependencies updated for CVE fixes
>
> **Download pre-built JAR (includes DB2 JCC driver):**
> ```
> https://github.com/abij/ReplicaDB/releases
> ```
>
> **Quick start — replicate any JDBC source to ADLS Gen2 Parquet:**
> ```bash
> java -jar ReplicaDB-0.18.0-jar-with-dependencies.jar \
>   --source-connect "jdbc:db2://host:50000/dbname" \
>   --source-user <user> --source-password <pass> \
>   --source-table MY_TABLE \
>   --sink-connect "abfss://filesystem@account.dfs.core.windows.net/data/output.parquet" \
>   --sink-file-format parquet \
>   --jobs 4
> ```
>
> Authentication uses the [Default Azure credential chain](https://learn.microsoft.com/en-us/azure/developer/java/sdk/identity-azure-hosted-auth) (Azure CLI, managed identity, service principal). See [ADLS Gen2 sink configuration](#adls-gen2-sink) below for all options.

# Fork Changelog

## v0.18.0 (abij fork)

### Features
- **ADLS Gen2 sink** — new `ADLSGen2Manager` supporting Parquet, CSV, and ORC output to Azure Data Lake Storage Gen2 (`abfss://filesystem@account.dfs.core.windows.net/path`)
- **`--sink-stats-file`** — CLI flag to write per-task replication statistics (rows, duration, file path) as JSON to a local file or ADLS Gen2 path
- **DB2 partition column exclusion** — `RN` partition column is automatically excluded from sink output
- **Windows path support** — drive letters (e.g. `C:\...`) are treated as local paths, not URI schemes

### Build & Dependencies
- **Java 21 build, Java 11 runtime** — `maven.compiler.release=11`, compiled with JDK 21
- **Dependency CVE fixes** — upgraded all dependencies to latest versions:
  - `aws-java-sdk-bom` 1.11.106 → 1.12.797
  - `mariadb-java-client` 2.7.3 → 2.7.12
  - `commons-cli` 1.4 → 1.11.0
  - `kafka-clients` 3.9.1 → 4.3.0
  - `jackson-databind` 2.17.2 → 2.19.0 (+ all jackson modules aligned)
  - `guava` 32.1.3 → 33.4.8
  - `sqlite-jdbc` 3.41.2.2 → 3.49.1.0
  - `mysql-connector-j` 8.2.0 → 9.3.0
  - `sentry` 5.1.2 → 8.16.0
  - `parquet-hadoop` 1.14.2 → 1.17.1
  - `hadoop-common` 3.4.0 → 3.5.0
  - `orc-core` 1.6.7 → 1.9.8 (+ explicit `hive-storage-api` 2.8.1)
  - Log4j 2.25.x → 2.26.0 (all modules aligned)
  - Azure SDK: `azure-storage-file-datalake` 12.22.0 → 12.23.0, `azure-identity` 1.15.0 → 1.16.2
  - Transitive CVE fixes (round 1): `commons-compress` 1.28.0, `netty` 4.1.121.Final, `httpclient` 4.5.14, `commons-logging` 1.3.6, `avro` 1.12.1, `commons-net` 3.13.0
  - Transitive CVE fixes (round 2, mend.io scan): `netty` 4.1.121 → 4.1.135.Final, `jackson-core/databind` 2.19.0 → 2.19.4, `reactor-netty-http` 1.0.48 → 1.3.6, `bcprov-jdk18on` 1.82 → 1.84
  - Note: `jetty-http` 9.4.58 (from `hadoop-common`) — Jetty 9.4.x is EOL with no upstream fix; risk accepted as it's only used internally by Hadoop's REST API
- **Maven plugins updated** — `maven-compiler-plugin` 3.7.0 → 3.15.0, `maven-surefire-plugin` 2.22.1 → 3.5.6

### Fixes
- `fix(adls2)`: treat Windows drive letters (`C:`) as local paths, not URI schemes
- `fix(db2)`: exclude internal `RN` partition column from sink output
- `fix(parquet)`: replace Hadoop `Path` with `LocalOutputFile` to fix Windows path errors

---

ReplicaDB is a high-performance, open-source command-line tool for bulk data replication between heterogeneous databases. It enables efficient ETL/ELT workflows by transferring data in parallel between Oracle, PostgreSQL, MySQL, MongoDB, SQL Server, and other databases without requiring database agents or triggers.

ReplicaDB supports a wide range of data sources including relational databases (Oracle, PostgreSQL, MySQL, MariaDB, SQL Server, SQLite, IBM DB2 LUW and DB2 for i), NoSQL databases (MongoDB), data virtualization platforms (Denodo), file formats (CSV), cloud storage (Amazon S3), and streaming platforms (Kafka). Any JDBC-compliant database is also supported with some limitations.

The tool is **cross-platform** compatible with Windows, Linux, and macOS, and leverages **parallel data transfer** for optimal performance and system utilization during large-scale data migrations and synchronization tasks.

<br>

![ReplicaDB-Conceptual](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/ReplicaDB-Conceptual.jpg)


# Why ReplicaDB

ReplicaDB addresses common gaps in existing database replication tools by providing:

- **Open Source:** Transparent development and community-driven improvements
- **Cross-Platform:** Java-based solution compatible with Linux, Windows, and macOS
- **Heterogeneous Support:** Works with SQL, NoSQL, and persistent stores like CSV, Amazon S3, or Kafka
- **Simple Architecture:** Standalone command-line tool without requiring database agents
- **High Performance:** Optimized for bulk data transfer with large datasets
- **Non-Intrusive:** Focused on batch replication without requiring database triggers or CDC installation

## Comparison with Alternatives

Common alternatives and how ReplicaDB differs:

- **SymmetricDS**: A comprehensive CDC solution with database triggers. While feature-rich, it requires installation and maintenance of capture tables in source databases, making it more intrusive for batch replication scenarios.
- **Sqoop**: Designed specifically for Hadoop ecosystems, limiting its use in other environments where Hadoop infrastructure is not available.
- **Pentaho and Talend**: Full-featured ETL platforms that require custom development for each replication job, increasing complexity and maintenance overhead for straightforward data transfer tasks.

**Feature Comparison**

| Feature | SymmetricDS | Sqoop | Pentaho/Talend | ReplicaDB |
|---------|-------------|-------|----------------|--------|
| Database Agents Required | Yes | No | No | **No** |
| Triggers in Source DB | Yes | No | No | **No** |
| Heterogeneous Databases | Limited | No | Yes | **Yes** |
| Hadoop Requirement | No | Yes | No | **No** |
| Custom Development per Job | Low | Low | High | **None** |
| Parallel Transfer | Yes | Yes | Yes | **Yes** |
| Open Source | Yes | Yes | Yes | **Yes** |


# Installation

## Prerequisites

Before installing ReplicaDB, ensure you have:

- **Java Runtime**: Java JDK or JRE 11 or higher installed and configured
- **Network Connectivity**: Reliable network access to both source and sink databases
- **Database Credentials**: Appropriate permissions on both databases:
  - **Source database**: SELECT permissions on tables to replicate
  - **Sink database**: INSERT, UPDATE, DELETE, and CREATE TABLE permissions
- **(Optional)** Docker or Podman for containerized deployment

## Stand Alone

### System Requirements

ReplicaDB is written in Java and requires a Java Runtime Environment (JRE) Standard Edition (SE) or Java Development Kit (JDK) Standard Edition (SE) version 11 or above. The minimum system requirements are:

*   Java SE Runtime Environment 11 or above
*   Memory - 256 MB minimum, 1 GB recommended for large datasets

### Install

Download the latest release from GitHub and extract the archive:

```bash
$ curl -o ReplicaDB-0.18.0.tar.gz -L "https://github.com/osalvador/ReplicaDB/releases/download/v0.18.0/ReplicaDB-0.18.0.tar.gz"
$ tar -xvzf ReplicaDB-0.18.0.tar.gz
$ ./bin/replicadb --help
```

### JDBC Drivers

ReplicaDB already comes with all the JDBC drivers for the [Compatible Databases](#compatible-databases). But you can use ReplicaDB with any JDBC-compliant database.

First, download the appropriate JDBC driver for the type of database you want to use, and install the `.jar` file in the `$REPLICADB_HOME/lib` directory. Each driver `.jar` file also has a specific driver class that defines the entry-point to the driver.

If your database is JDBC-compliant and not appear in the [Compatible Databases](#compatible-databases) list, you must set the driver class name in the configuration properties as [extra JDBC parameter](https://osalvador.github.io/ReplicaDB/docs/docs.html#32-connecting-to-a-database-server).

For example, to replicate a DB2 database table as both source and sink

```properties
######################## ReplicadB General Options ########################
mode=complete
jobs=1
############################# Source Options ##############################
source.connect=jdbc:db2://localhost:50000/testdb
source.user=${DB2USR}
source.password=${DB2PASS}
source.table=source_table
source.connect.parameter.driver=com.ibm.db2.jcc.DB2Driver
############################# Sink Options ################################
sink.connect=jdbc:db2://localhost:50000/testdb
sink.user=${DB2USR}
sink.password=${DB2PASS}
sink.table=sink_table
sink.connect.parameter.driver=com.ibm.db2.jcc.DB2Driver
```

## Docker

For containerized deployments or environments without Java installed, ReplicaDB is available as a Docker image.

```bash
$ docker run \
    -v /tmp/replicadb.conf:/home/replicadb/conf/replicadb.conf \
    osalvador/replicadb
```

Visit the [project homepage on Docker Hub](https://hub.docker.com/r/osalvador/replicadb) for more information. 

## Podman 

For Red Hat Enterprise Linux and Fedora environments, ReplicaDB provides a container image based on Red Hat Universal Base Image (UBI) 8, which is optimized for enterprise security and compliance.

```bash
$ podman run \
    -v /tmp/replicadb.conf:/home/replicadb/conf/replicadb.conf:Z \
    osalvador/replicadb:ubi8-latest
```

**Note**: The `:Z` flag relabels the volume for SELinux compatibility. See [Podman documentation](https://docs.podman.io/en/latest/markdown/podman-run.1.html#volume-v-source-volume-host-dir-container-dir-options) for details on volume mounting with SELinux.

# Full Documentation

You can find the full ReplicaDB documentation here: [Docs](https://osalvador.github.io/ReplicaDB/docs/docs.html)

# Configuration Wizard

You can create a configuration file for a ReplicaDB process by filling out a simple form: [ReplicaDB configuration wizard](https://osalvador.github.io/ReplicaDB/wizard/index.html)

# Quick Start Examples

## Oracle to PostgreSQL

> **Security Note**: The examples below use environment variables for credentials. Never hard-code passwords in scripts or command history.

**Prerequisites**:
- Source table must exist and be accessible with SELECT permissions
- Sink table must exist with a compatible schema
- For `incremental` mode, sink table must have primary keys defined

```bash
$ replicadb --mode=complete -j=1 \
--source-connect=jdbc:oracle:thin:@$ORAHOST:$ORAPORT:$ORASID \
--source-user=$ORAUSER \
--source-password=$ORAPASS \
--source-table=dept \
--sink-connect=jdbc:postgresql://$PGHOST/osalvador \
--sink-table=dept
2026-01-28 10:15:23,808 INFO  ReplicaTask:36: Starting TaskId-0
2026-01-28 10:15:24,650 INFO  SqlManager:197: TaskId-0: Executing SQL statement: SELECT /*+ NO_INDEX(dept)*/ * FROM dept where ora_hash(rowid,0) = ?
2026-01-28 10:15:24,650 INFO  SqlManager:204: TaskId-0: With args: 0,
2026-01-28 10:15:24,772 INFO  ReplicaDB:89: Total process time: 1302ms
```

Alternatively, use a configuration file to simplify repeated operations:

```properties
######################## ReplicadB General Options ########################
mode=complete
jobs=1
############################# Source Options ##############################
source.connect=jdbc:oracle:thin:@${ORAHOST}:${ORAPORT}:${ORASID}
source.user=${ORAUSER}
source.password=${ORAPASS}
source.table=dept
############################# Sink Options ################################
sink.connect=jdbc:postgresql://${PGHOST}/osalvador
sink.table=dept
```

```bash
$ replicadb --options-file replicadb.conf
```

![ReplicaDB-Ora2PG.gif](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/ReplicaDB-Ora2PG.gif)

## PostgreSQL to Oracle

```bash
$ replicadb --mode=complete -j=1 \
--sink-connect=jdbc:oracle:thin:@$ORAHOST:$ORAPORT:$ORASID \
--sink-user=$ORAUSER \
--sink-password=$ORAPASS \
--sink-table=dept \
--source-connect=jdbc:postgresql://$PGHOST/osalvador \
--source-table=dept \
--source-columns=dept.*
2026-01-28 10:20:35,334 INFO  ReplicaTask:36: Starting TaskId-0
2026-01-28 10:20:35,440 INFO  SqlManager:131 TaskId-0: Executing SQL statement: SELECT  * FROM dept OFFSET ?
2026-01-28 10:20:35,441 INFO  SqlManager:204: TaskId-0: With args: 0,
2026-01-28 10:20:35,550 INFO  OracleManager:98 Inserting data with this command: INSERT INTO /*+APPEND_VALUES*/ ....
2026-01-28 10:20:35,552 INFO  ReplicaDB:89: Total process time: 1007ms
```

# Compatible Databases

| Persistent Store        |          Source          |    Sink Complete   |   Sink Complete-Atomic    |     Sink Incremental     | Sink Bandwidth Throttling |
|-------------------------|:------------------------:|:------------------:|:-------------------------:|:------------------------:|:-------------------------:|
| Oracle                  |    :heavy_check_mark:    | :heavy_check_mark: |    :heavy_check_mark:     |    :heavy_check_mark:    |     :heavy_check_mark:    |
| MySQL                   |    :heavy_check_mark:    | :heavy_check_mark: |    :heavy_check_mark:     |    :heavy_check_mark:    |     :heavy_check_mark:    |
| MariaDB                 |    :heavy_check_mark:    | :heavy_check_mark: |    :heavy_check_mark:     |    :heavy_check_mark:    |     :heavy_check_mark:    |
| PostgreSQL              |    :heavy_check_mark:    | :heavy_check_mark: |    :heavy_check_mark:     |    :heavy_check_mark:    |     :heavy_check_mark:    |
| IBM DB2 LUW             |    :heavy_check_mark:    | :heavy_check_mark: |    :heavy_check_mark:     |    :heavy_check_mark:    |     :heavy_check_mark:    |
| IBM DB2/i               |    :heavy_check_mark:    | :heavy_check_mark: |    :heavy_check_mark:     |    :heavy_check_mark:    |     :heavy_check_mark:    |
| SQLite                  |    :heavy_check_mark:    | :heavy_check_mark: | :heavy_multiplication_x:  |    :heavy_check_mark:    |     :heavy_check_mark:    |
| SQL Server              |    :heavy_check_mark:    | :heavy_check_mark: |    :heavy_check_mark:     |    :heavy_check_mark:    |  :heavy_multiplication_x: |
| MongoDB                 |    :heavy_check_mark:    | :heavy_check_mark: | :heavy_multiplication_x:  |    :heavy_check_mark:    |     :heavy_check_mark:    |
| Denodo                  |    :heavy_check_mark:    |         N/A        |            N/A            |           N/A            |            N/A            |
| CSV                     |    :heavy_check_mark:    | :heavy_check_mark: |            N/A            |    :heavy_check_mark:    |     :heavy_check_mark:    |
| Kafka                   | :heavy_multiplication_x: |         N/A        |            N/A            |    :heavy_check_mark:    |     :heavy_check_mark:    |
| Amazon S3               | :heavy_multiplication_x: | :heavy_check_mark: |            N/A            |           N/A            |     :heavy_check_mark:    |
| Azure ADLS Gen2 *(fork)*| :heavy_multiplication_x: | :heavy_check_mark: |            N/A            |           N/A            |     :heavy_check_mark:    |
| JDBC-Compliant database |    :heavy_check_mark:    | :heavy_check_mark: | :heavy_multiplication_x:  | :heavy_multiplication_x: |     :heavy_check_mark:    |

See [DB2 Documentation](https://osalvador.github.io/ReplicaDB/docs/docs.html) for driver installation and platform-specific details.

# ADLS Gen2 Sink

> This feature is available in this fork only.

Sink URI format:
```
abfss://<filesystem>@<account>.dfs.core.windows.net/<path/to/output.parquet>
```

File formats: `parquet` (default: Snappy compressed) and `csv`.

## Authentication

Resolved in priority order via `sink.connect.parameter.*`:

| Priority | Method | Parameters |
|----------|--------|------------|
| 1 | Storage account key | `accountKey=<key>` |
| 2 | Service principal | `tenantId=`, `clientId=`, `clientSecret=` |
| 3 | Default Azure credential | *(none — uses Azure CLI / managed identity / env vars)* |

## Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `accountKey` | Storage account key | — |
| `tenantId` / `clientId` / `clientSecret` | Service principal | — |
| `endpoint` | Override service endpoint (Azurite / sovereign cloud) | derived from URI |
| `parquet.compression` | `SNAPPY`, `GZIP`, `ZSTD`, `UNCOMPRESSED` | `SNAPPY` |
| `statsFile` | Path within the filesystem to write the stats JSON | `<dir>/_replicadb_stats.json` |

## Example options file

```properties
# replicadb.conf
source.connect=jdbc:db2://dbhost:50000/mydb
source.user=myuser
source.password=secret
source.table=MY_TABLE

sink.connect=abfss://landing@mystorageaccount.dfs.core.windows.net/data/output.parquet
sink.file.format=parquet

# Auth — omit to use Azure CLI / managed identity
sink.connect.parameter.tenantId=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
sink.connect.parameter.clientId=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
sink.connect.parameter.clientSecret=<secret>

# Optional
sink.connect.parameter.parquet.compression=SNAPPY
sink.connect.parameter.statsFile=meta/run_stats.json
```

Output files with `--jobs 4`:
```
data/output_0.snappy.parquet
data/output_1.snappy.parquet
data/output_2.snappy.parquet
data/output_3.snappy.parquet
meta/run_stats.json
```

## Docker

A ready-to-use image can be built from `Dockerfile.adls2` in this repository. It downloads the fat JAR (including DB2 JCC driver) from the GitHub release at build time — no local Java installation required.

```bash
# Build the image
docker build -f Dockerfile.adls2 -t replicadb-adls2:preview .

# Run with an options file
docker run --rm \
  -v /path/to/replicadb.conf:/conf/replicadb.conf \
  replicadb-adls2:preview \
  --options-file /conf/replicadb.conf
```

### Azure CLI authentication

If you authenticate via `az login` on the host, mount the Azure credential cache read-only into the container. The `DefaultAzureCredential` chain will find it automatically — no credentials needed in the options file:

```bash
docker run --rm \
  -v ~/.azure:/home/replicadb/.azure:ro \
  -v /path/to/replicadb.conf:/conf/replicadb.conf \
  replicadb-adls2:preview \
  --options-file /conf/replicadb.conf
```

This works for local development and CI pipelines where `az login` (or `az login --service-principal`) has already been run on the host.

For AKS or other Azure-hosted environments, workload identity or managed identity is picked up automatically without any mount.

# Roadmap

Features: 
- Replicate multiple tables in a single run
- Scheduling
- Web interface
- Server mode with API 
- Kubernetes compliant

New Databases: 
- Elasticsearch
- Redis
- GCP BigQuery
- Azure Synapse

# Contributing

We welcome contributions to ReplicaDB! Whether you're fixing bugs, adding features, or improving documentation, your help is appreciated.

**How to Contribute**:
  
1. Fork the repository: https://github.com/osalvador/ReplicaDB
2. Create your feature branch: `git checkout -b feature/your-feature-name`
3. Commit your changes: `git commit -am 'Add feature description'`
4. Push to the branch: `git push origin feature/your-feature-name`
5. Create a Pull Request

**Contribution Guidelines**:

- Follow existing code style and conventions
- Add tests for new functionality
- Update documentation to reflect your changes
- Ensure all tests pass before submitting PR
- Keep pull requests focused on a single feature or fix

For detailed guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md) (when available).

# License

ReplicaDB is open source software released under the Apache License 2.0. You are free to use, modify, and distribute this software for both commercial and non-commercial purposes, subject to the terms and conditions of the license.

**Key points**:
- Free for commercial and personal use
- Modification and distribution permitted
- Must include license and copyright notices
- Provided "as is" without warranty

For complete license terms, see the [LICENSE](https://github.com/osalvador/ReplicaDB/blob/master/LICENSE) file in the repository.
