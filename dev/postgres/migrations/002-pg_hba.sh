#!/usr/bin/env bash
set -e

echo "host all all 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"
# Allow replication connections specifically (tighten CIDR to your network)
echo "host replication all 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"