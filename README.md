# Librarian

Librarian is a modern cloud-native kafka connect alternative. Librarian uses native data replication technologies (such as Postgres Replication and Mongo Changestreams) to efficiently archive data.

Think of Librarian as Kafka Connect CDC source for modern data world.

## What makes librarian Modern?

- Librarian is distributed as a single binary with no external dependencies.
- Librarian includes data integrity checks to ensure correctness.
- Librarian includes data-oriented observability out of the box, including latency and completeness.
- Librarian is runnable as a daemon or as a batch process.

## What Makes librarian cloud-native?

- Librarian includes modern telemetry including metrics and tracing natively. 
- Librarian is performant and efficient, deployable on modest hardware instances. 


# Features and Roadmap.
- [x] Postgres Snapshot
- [x] Parquet Serialization
- [x] Parquet Schema Generation from sql CREATE Statement
- [x] S3 Persistence
- [x] Data Observability - Snapshot Completeness
- [ ] Mongo Snapshot
- [ ] Postgres CDC
- [ ] Mongo CDC
- [ ] Ephemeral change streams
- [ ] Debezium Messages Format Compatibility


## Data Snapshots

Snapshots capture the state of data at a specific point in time. They are simple to create and maintain because each snapshot is an isolated copy. This reduces complexity compared to incremental or differential backups.

Snapshots are operationally forgiving. If a snapshot fails or becomes corrupted, previous snapshots remain intact. Each snapshot is independent, allowing easy recovery without affecting other snapshots. This independence simplifies debugging and rollback processes.

By using snapshots, you ensure data durability and maintain a reliable history of changes.

Librarian supports data snapshots for Postgres tables and archiving the snapshot using Parquet, either locally or remotely in s3. 


# Quickstart 

The easiest way to get started with librarian is to clone this repo. 

## Tutorial: Generate a Postgres Parquet Snapshot Locally

This tutorial will perform a local snapshot using a sample postgres dataset. The end result 
will be a valid local parquet file that contains a snapshot of the postgres table.

- Start Postgres Locally
```
docker-compose -f dev/compose.yml up -d
```
- Use librarian to snapshot postgres property sales test dataset and save it locally
```
time go run cmd/librarian/main.go archiver snapshot -c dev/examples/property-sales.snapshot.yml
```

- Check librarian stdout for the location of the parquet snapshot file
<img width="1512" alt="Screenshot 2024-12-10 at 7 54 46 PM" src="https://github.com/user-attachments/assets/91293df5-64eb-46cd-99e9-9972aceaf409">


- Query the Parquet file using duckdb :)
<img width="1512" alt="Screenshot 2024-12-10 at 7 55 44 PM" src="https://github.com/user-attachments/assets/6ecca3f7-a4da-4cdc-9814-9a4ac910dd50">

- Inspect the snapshot catalog
```
cat data/property_sales/7900e2f0-b75a-11ef-8e40-9e78fe1d02fa/catalog.json| jq .

{
  "id": "7900e2f0-b75a-11ef-8e40-9e78fe1d02fa",
  "start_time": "2024-12-11T00:54:27.725337Z",
  "end_time": "2024-12-11T00:54:32.492852Z",
  "source": "public.property_sales",
  "num_source_records": 1097629,
  "num_records_processed": 1097629,
  "success": true
}
```


