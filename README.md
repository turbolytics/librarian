# Librarian

Librarian is a modern cloud-native kafka connect alternative. Librarian uses native data replication technologies (such as Postgres Replication and Mongo Changestreams) to efficiently archive data.

Think of Librarian as Kafka Connect CDC source for modern data world. 

## What makes librarian Modern?

- Librarian is distributed as a single binary with no external dependencies.
- Librarian includes data-oriented observability out of the box, including latency and completeness.
- Librarian is runnable as a daemon or as a batch process.

## What Makes libraian cloud-native?

- Librarian includes modern telemetry including metrics and tracing natively. 
- Librarian is performant and efficient, deployable on modest hardware instances. 


# Features and Roadmap.
- [x] Postgres Snapshot
- [x] Parquet Serialization
- [ ] S3 Persistence
- [ ] Data Observability - Latency & Completeness
- [ ] Mongo Snapshot
- [ ] Postgres CDC
- [ ] Mongo CDC
- [ ] Ephemeral change streams
- [ ] Debezium Messages Format Compatibility

