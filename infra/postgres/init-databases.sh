#!/bin/bash
# Runs once, on first start of an empty Postgres data directory.
# Creates one database and one user per service. Each user can connect only to its own
# database, which enforces ADR-0001 (no service reads another's data) at the database level.
set -euo pipefail

create_service_db() {
  local db="$1" user="$2" password="$3"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
	CREATE USER ${user} WITH PASSWORD '${password}';
	CREATE DATABASE ${db} OWNER ${user};
	REVOKE CONNECT ON DATABASE ${db} FROM PUBLIC;
	GRANT CONNECT ON DATABASE ${db} TO ${user};
SQL
  echo "created database ${db} owned by ${user}"
}

create_service_db order_db     order_svc     "${ORDER_DB_PASSWORD}"
create_service_db payment_db   payment_svc   "${PAYMENT_DB_PASSWORD}"
# inventory_db is created last. The compose healthcheck waits for it, so "healthy" means init finished.
create_service_db inventory_db inventory_svc "${INVENTORY_DB_PASSWORD}"
