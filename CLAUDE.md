# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build
mvn clean install
mvn clean package -DskipTests   # skip tests
mvn clean install -P release    # release build (creates tar.gz/zip distributions)

# Run all tests
mvn test

# Run specific test class or method
mvn test -Dtest=Oracle2MySQLTest
mvn test -Dtest=Oracle2MySQLTest#testCompleteMode

# Run with release artifact
./release.sh 0.18.0             # tags and triggers CI/CD release
```

Tests use TestContainers and require Docker running locally. Containers are reused across test runs (configured in `src/test/resources/testcontainers.properties`).

## Architecture

ReplicaDB is a zero-agent, CLI-first bulk data transfer tool that replicates data between heterogeneous databases and storage systems. It is a **point-to-point replication tool**, not an ETL platform — no transformations, orchestration, or scheduling.

### Core Flow

```
ReplicaDB.main()
  └── ReplicaDB.processReplica(options)
        ├── ManagerFactory → creates source/sink Manager instances
        └── ExecutorService (N worker threads = --jobs)
              └── ReplicaTask (per thread)
                    ├── source.readTable(partitionIndex)   → ResultSet
                    └── sink.insertDataToTable(resultSet)  → row count
```

### Manager Pattern (Database Abstraction)

All database-specific logic lives in `org.replicadb.manager`. The hierarchy is:

```
ConnManager (abstract base)
└── SqlManager (generic JDBC/SQL)
    ├── OracleManager      — ORA_HASH partitioning, NO_INDEX hint, LOB/SDO_GEOMETRY
    ├── PostgresqlManager  — binary COPY protocol (10x faster inserts)
    ├── MySQLManager       — backtick escaping, streaming reads
    ├── SQLServerManager   — Bulk Insert API, CHECKSUM partitioning
    ├── MongoDBManager     — aggregation pipelines, BSON types
    ├── S3Manager          — CSV/Parquet output
    ├── KafkaManager       — partitioned topic sink
    └── ...
```

`ManagerFactory` selects the correct manager via JDBC URL pattern matching.

**Rule**: Database-specific logic always goes in `XYZManager`, never in `SqlManager` or `ReplicaDB`.

### Parallelism

Each worker thread gets its own JDBC connection and reads a partition of the source data. Partitioning uses database-native hash functions: `ORA_HASH` (Oracle), `HASH`/`CHECKSUM` (SQL Server), `OFFSET/LIMIT` (PostgreSQL/MySQL). No connection pooling — one connection per thread per job run.

### Replication Modes

- `complete` — full table replace
- `complete-atomic` — atomic full table replace (transaction-based)
- `incremental` — upsert based on timestamp/sequence columns

### Key Extension Points

When adding a new database:
1. Create `XYZManager extends SqlManager` in `org.replicadb.manager`
2. Override `getDriverClass()`, `escapeColName()`, `readTable()` (partitioning), and optionally `insertDataToTable()` (bulk API)
3. Register in `ManagerFactory` via JDBC URL pattern switch
4. Add TestContainers config in `src/test/java/org/replicadb/config/`
5. Add SQL fixtures in `src/test/resources/{database}/` and sink schemas in `src/test/resources/sinks/`
6. Write integration tests in `src/test/java/org/replicadb/{database}/` covering complete/incremental/parallel modes

## Test Organization

Tests are named `{Source}2{Sink}Test.java` (e.g., `Oracle2MySQLTest`, `MariaDB2CsvFileTest`). The pattern:
- `@BeforeAll` — start singleton container
- `@BeforeEach` — fresh connections
- `@AfterEach` — truncate sink tables
- Assert row counts via `ReplicaDB.processReplica(new ToolOptions(args))` returning exit code 0

SQL fixtures: `src/test/resources/{database}/{database}-source.sql`
Sink schemas: `src/test/resources/sinks/{database}-sink.sql`

## Git Commit Conventions

Follows Conventional Commits:

```
<type>(<optional-scope>): <subject>
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `revert`
Common scopes: `postgres`, `oracle`, `sqlserver`, `mysql`, `mongodb`, `s3`, `kafka`, `cli`, `manager`

Subject line: imperative mood, lowercase after colon, no period, ≤50 chars.

## Important Constraints

- **Never break CLI argument compatibility** — add new args with backward-compatible defaults, never rename/remove
- **Integration tests are required** — unit tests alone are insufficient for database logic; always test with TestContainers
- **No database-specific logic in `SqlManager` or `ReplicaDB`** — use a Manager subclass
- **DB2 and Denodo JDBC drivers are not bundled** (licensing) — users provide them in `$REPLICADB_HOME/lib/`
- Source uses read-only, auto-commit disabled (streaming). Sink uses manual commit after batch; rollback on any thread failure.
