# Security guide

What `docker-compose.security.yaml`, `.env` and `config/webserver_config.py` set up, and how to operate it.

## What is protected

| Area | Setup |
|---|---|
| **Port** | Airflow listens on internal port 9091 and publishes nothing. The only published port is **8443**, served by nginx. Port 8080 is not used. |
| **Network exposure** | nginx binds to `127.0.0.1` by default (this machine only). |
| **Encryption in transit** | TLS 1.2/1.3 at nginx, HSTS and security headers. Session cookies are `Secure`, `HttpOnly`, `SameSite=Lax`. |
| **Login** | FAB auth manager with real users and roles (Admin, Op, User, Viewer). No self-registration. Sessions last 8 hours. |
| **Secrets** | Random JWT secret, API secret key and Fernet key in `.env`. The official compose file ships a fixed default JWT secret, which this setup replaces. |
| **API hardening** | Config display, Swagger UI and stack traces are off; confirmation dialog for DAG changes; example DAGs off. |
| **Proxy headers** | The API server runs with `--proxy-headers` so it trusts nginx's `X-Forwarded-Proto: https` (without it Airflow redirects to `http://` and nginx answers 400). |
| **Proxy rules** | `/execution/` (worker-only API) returns 404 from outside; `/auth/*` is rate limited (20 requests/minute per IP) against password guessing. |
| **Databases** | The config DB password is random and the port is not published. Airflow's own Postgres and Redis are also not published by the official compose file. |

## Start and verify

Follow [GETTING_STARTED.md](GETTING_STARTED.md). Quick checks:

```bash
curl -k -s -o /dev/null -w "%{http_code}\n" https://localhost:8443/   # 200 or 302 over TLS (do not use -I: HEAD gives 405)
curl -I http://localhost:8080/            # should fail to connect
curl -k -i https://localhost:8443/execution/   # 404
```

## Users and roles

```bash
docker compose exec airflow-apiserver airflow users list
docker compose exec airflow-apiserver airflow users create --username alice --firstname Alice --lastname Doe --role Viewer --email alice@example.com
docker compose exec airflow-apiserver airflow users add-role --username alice --role User
docker compose exec airflow-apiserver airflow users reset-password --username alice
docker compose exec airflow-apiserver airflow users delete --username alice
```

| Role | Use for |
|---|---|
| Viewer | Read-only access to DAGs, runs and logs |
| User | Viewer plus create/edit/delete/trigger DAG runs |
| Op | User plus connections, variables, pools, config |
| Admin | Everything, including user management |

Give most people Viewer or User. Keep Admin and Op for the platform team, because Op can read and edit connections.

## Remote access and certificates

- **Allow other machines:** set `AIRFLOW_BIND_ADDRESS=0.0.0.0` in `.env`, then `docker compose up -d`. Restrict with a firewall or VPN, and use a real certificate.
- **Change the public port:** `AIRFLOW_PUBLIC_PORT` in `.env`, and update `AIRFLOW_BASE_URL` to match.
- **Real certificate:** replace `security/certs/airflow.crt` and `airflow.key` with files from your CA or Let's Encrypt (the file names must stay), set `AIRFLOW_BASE_URL=https://your.host.name`, then `docker compose restart nginx`. The generated certificate is self-signed and valid for 365 days, for development only.

## Rotating secrets

- **JWT secret / API secret key:** change `AIRFLOW_JWT_SECRET` or `AIRFLOW_API_SECRET_KEY` in `.env`, then `docker compose up -d --force-recreate`. Users are logged out. All components must share the same values, which the override guarantees.
- **Config DB password:** `ALTER USER postgres PASSWORD '...'` in the config DB, update `CONFIG_DB_PASSWORD` in `.env`, then recreate the containers.
- **Fernet key:** do not simply replace it, or stored connection passwords become unreadable. Put the new key first and the old key second (comma separated) in `AIRFLOW_FERNET_KEY`, recreate the containers, then run `docker compose exec airflow-apiserver airflow rotate-fernet-key`. Remove the old key afterwards.

## Single sign-on (recommended next)

Passwords in the Airflow database are fine for a start, but SSO is better: centralized joiners/leavers and MFA. `config/webserver_config.py` has a commented OAuth section. The FAB provider documentation ("Single Sign-On (SSO) Integration") covers Okta, Google and Azure, including mapping IdP groups to Airflow roles. Keycloak is another option through its own auth manager.

## Checklist before real use

- [ ] `.env` is not in Git (`.gitignore` already lists it); copies are stored in a secret manager
- [ ] Real TLS certificate in place
- [ ] Default users removed; only named admin accounts exist
- [ ] `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS` is not set anywhere
- [ ] Firewall allows only 8443 (and only from the networks that need it)
- [ ] Redis has persistence and, if the network is shared, a password
- [ ] Backups of the config DB and Airflow DB; restore tested
- [ ] Connections use least-privilege service accounts

## Not covered yet

- SSO / MFA (see above).
- Per-team DAG permissions: Phase 2 should map `team_name` to Airflow roles so teams see only their own DAGs. Airflow 3 may handle DAG-level permissions differently from Airflow 2, so verify against the docs when we get there.
- Redis password and TLS between containers, a secrets backend (Vault, GCP Secret Manager) instead of `.env`, network policies, audit log shipping.
- Kubernetes: this setup is for Docker Compose; on Kubernetes the same ideas apply through an ingress with TLS, Kubernetes secrets and the Helm chart settings.
