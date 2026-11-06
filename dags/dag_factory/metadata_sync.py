"""Config DB (Postgres) -> validate -> metadata-cache (Redis). Shared by the CLI and the Airflow sync DAG.
Heavy imports (psycopg, redis) are lazy so importing this module is cheap."""
import hashlib, json, os, time, zlib

from .validation import validate, sort_groups, ConfigError


def shard_of(dag_id, n):
    return zlib.crc32(dag_id.encode()) % n


def load_payloads(cur, only=None):
    """One plain SELECT on the v_dag_metadata view (sql/003_views.sql); joins live in Postgres."""
    if only:
        cur.execute("SELECT * FROM v_dag_metadata WHERE dag_id = %s", (only,))
    else:
        cur.execute("SELECT * FROM v_dag_metadata ORDER BY dag_id")
    dags = {r["dag_id"]: r for r in cur.fetchall()}            # each row IS the payload
    for p in dags.values():
        try:
            p["groups"] = sort_groups(p["groups"])             # parents first
        except ConfigError:
            pass                                               # validate() reports it per DAG
    return dags


def publish(r, valid, active_ids, n_shards, only=None):
    """Delta-write validated payloads to the cache in ONE atomic transaction.
    Returns ({dag_id: (hash, json)}, [removed ids])."""
    known = {k.decode(): v.decode() for k, v in r.hgetall("metadata:hashes").items()}
    written, pipe = {}, r.pipeline()
    for dag_id, p in valid.items():
        text = json.dumps(p, sort_keys=True, default=str)
        h = hashlib.sha256(text.encode()).hexdigest()
        if known.get(dag_id) != h:                             # unchanged -> untouched -> no new DAG version
            pipe.hset(f"metadata:dags:{shard_of(dag_id, n_shards)}", dag_id, text)
            pipe.hset("metadata:hashes", dag_id, h)
            written[dag_id] = (h, text)
    if only:
        stale = [only] if only in known and only not in active_ids else []
    else:
        stale = [d for d in known if d not in active_ids]
    for d in stale:                                            # deactivated / deleted in config DB
        pipe.hdel(f"metadata:dags:{shard_of(d, n_shards)}", d)
        pipe.hdel("metadata:hashes", d)
    pipe.hset("metadata:meta", mapping={"last_sync": int(time.time()), "shards": n_shards})
    pipe.execute()
    return written, stale


def run_sync(dag_id=None, dry_run=False, rebuild=False):
    """Env: CONFIG_DB_DSN, CONFIG_CACHE_URL, DAG_SHARDS (default 1), GENERATOR_FILE (optional).
    Returns a summary dict. Invalid DAGs keep their last good cache entry and are listed in 'failed'."""
    import psycopg, redis
    from psycopg.rows import dict_row
    n = int(os.environ.get("DAG_SHARDS", "1"))
    r = redis.Redis.from_url(os.environ["CONFIG_CACHE_URL"], socket_timeout=10)
    prev = r.hget("metadata:meta", "shards")
    if rebuild and not dry_run:
        for k in r.scan_iter("metadata:*"):
            r.delete(k)
    elif prev and int(prev) != n:
        raise RuntimeError(f"DAG_SHARDS={n} but cache was built with {int(prev)}: run the CLI with --rebuild")
    with psycopg.connect(os.environ["CONFIG_DB_DSN"], row_factory=dict_row, autocommit=True,
                         connect_timeout=15) as conn:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(727001) AS ok")        # one sync at a time (CLI or DAG)
        if not cur.fetchone()["ok"]:
            return {"status": "skipped", "reason": "another sync is running",
                    "written": [], "removed": [], "failed": []}
        try:
            def log(d, s, m=""):
                if not dry_run:
                    cur.execute("INSERT INTO sync_log(dag_id,status,message) VALUES (%s,%s,%s)", (d, s, m))

            payloads = load_payloads(cur, dag_id)
            valid, failed = {}, []
            for d, p in payloads.items():
                try:
                    validate(p)
                    valid[d] = p
                except (ConfigError, KeyError) as e:
                    failed.append({"dag_id": d, "error": str(e)})
                    log(d, "failed", str(e))
            if dry_run:
                return {"status": "dry_run", "valid": len(valid), "written": [], "removed": [], "failed": failed}
            written, stale = publish(r, valid, set(payloads), n, dag_id)
            for d, (h, text) in written.items():
                cur.execute("UPDATE dag_config SET content_hash=%s WHERE dag_id=%s", (h, d))
                cur.execute("INSERT INTO dag_config_history(dag_id,content_hash,payload) VALUES (%s,%s,%s)",
                            (d, h, text))
                log(d, "written")
            for d in stale:
                log(d, "removed")
            gen = os.environ.get("GENERATOR_FILE")             # optional nudge so the DAG processor reparses sooner
            if gen and (written or stale):
                try:
                    os.utime(gen)
                except OSError:
                    pass                                       # read-only / different filesystem: harmless
            return {"status": "ok", "valid": len(valid), "written": sorted(written), "removed": stale,
                    "failed": failed}
        finally:
            cur.execute("SELECT pg_advisory_unlock(727001)")
