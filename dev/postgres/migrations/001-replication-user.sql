-- Create a dedicated user for logical replication / Debezium
CREATE ROLE dbz WITH LOGIN PASSWORD 'dbzpass' REPLICATION;

-- Grant minimal privileges Debezium commonly needs
GRANT CONNECT ON DATABASE test TO dbz;
GRANT USAGE ON SCHEMA public TO dbz;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbz;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dbz;

-- Optional: create publication covering desired tables (or ALL TABLES)
-- Debezium can also create/require this, but doing it here is explicit:
\c test 
CREATE PUBLICATION librarian_pub_test_users FOR ALL TABLES;