#!/usr/bin/env bash
# Creates .env with random secrets and a self-signed TLS cert (dev). Never overwrites existing files.
set -euo pipefail
cd "$(dirname "$0")/.."

# Airflow containers must NOT run as root (the image refuses to run pip as root).
AUID=$(id -u)
if [ "$AUID" = "0" ]; then
  AUID=50000
  echo "running as root: using AIRFLOW_UID=50000 (never 0)"
fi
mkdir -p logs plugins
if [ "$(id -u)" = "0" ]; then
  chown -R 50000:0 logs plugins dags config     # container user 50000 must be able to write logs
fi

if [ -f .env ]; then
  echo ".env already exists - left untouched"
else
  FERNET=$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n')
  cat > .env <<ENV
COMPOSE_FILE=docker-compose.yaml:docker-compose.security.yaml
AIRFLOW_UID=$AUID
AIRFLOW_JWT_SECRET=$(openssl rand -hex 32)
AIRFLOW_API_SECRET_KEY=$(openssl rand -hex 32)
AIRFLOW_FERNET_KEY=$FERNET
FERNET_KEY=$FERNET
CONFIG_DB_PASSWORD=$(openssl rand -hex 16)
AIRFLOW_BASE_URL=https://localhost:8443
AIRFLOW_BIND_ADDRESS=127.0.0.1
AIRFLOW_PUBLIC_PORT=8443
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-fab psycopg[binary] redis apache-airflow-providers-google apache-airflow-providers-postgres
ENV
  chmod 600 .env
  echo "created .env with random secrets"
  if [ "$(id -u)" != "0" ]; then echo "note: if you later see 'Permission denied' on logs/, run: sudo chown -R $AUID:0 logs plugins"; fi
fi

mkdir -p security/certs
if [ -f security/certs/airflow.crt ]; then
  echo "TLS cert already exists - left untouched"
else
  openssl req -x509 -nodes -newkey rsa:2048 -days 365 \
    -keyout security/certs/airflow.key -out security/certs/airflow.crt \
    -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" 2>/dev/null
  chmod 600 security/certs/airflow.key
  echo "created self-signed cert (dev only - use a real certificate for production)"
fi
