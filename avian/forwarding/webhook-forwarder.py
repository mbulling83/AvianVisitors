#!/usr/bin/env python3
"""Offline-first forwarder for AvianVisitors / BirdNET-Pi.

Drains new detections from birds.db to a generic HTTPS webhook whenever the Pi
has connectivity. Each detection is POSTed as multipart/form-data (metadata
fields + the audio clip). Delivery is at-least-once with a durable rowid
bookmark, so a dropped link mid-backlog resumes cleanly with no duplicates and
no gaps. Edit the CONFIG constants below, then run via avian-webhook.service.
"""
import json
import os
import socket
import sqlite3
import time

# ---- CONFIG (edit these) ---------------------------------------------------
ENDPOINT_URL = "https://example.com/webhook"   # your HTTPS receiver
AUTH_TOKEN = ""                                # bearer token / shared secret
AUTH_HEADER = "Authorization"                  # header name to send the token in
AUTH_SCHEME = "Bearer"                          # set to "" to send the bare token
POLL_SECONDS = 30                               # how often to check connectivity
BATCH_SIZE = 25                                 # detections per drain pass
MIN_CONFIDENCE = 0.0                            # skip detections below this
DB_PATH = "~/BirdNET-Pi/scripts/birds.db"       # BirdNET-Pi detections DB
BIRDSONGS_DIR = "~/BirdSongs/Extracted/By_Date"  # extracted clips root
STATE_PATH = "~/avian-webhook-state.json"        # durable bookmark
HTTP_TIMEOUT = 30                                # seconds per request / connect
# ---------------------------------------------------------------------------


def read_bookmark(state_path):
    """Return the last forwarded rowid, or 0 if missing/corrupt."""
    try:
        with open(state_path) as f:
            return int(json.load(f)["last_forwarded_id"])
    except (FileNotFoundError, ValueError, KeyError, json.JSONDecodeError):
        return 0


def write_bookmark(state_path, rowid):
    """Atomically persist the high-water-mark."""
    tmp = state_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_forwarded_id": int(rowid)}, f)
    os.replace(tmp, state_path)


def select_new_detections(db_path, after_rowid, batch_size):
    """Read up to batch_size detections with rowid > after_rowid, in order."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        con.row_factory = sqlite3.Row
        cur = con.execute(
            "SELECT rowid, * FROM detections WHERE rowid > ? "
            "ORDER BY rowid ASC LIMIT ?",
            (after_rowid, batch_size),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        con.close()


def resolve_audio_path(birdsongs_dir, row):
    """Path of the extracted clip for a detection (may not exist)."""
    species_dir = row["Com_Name"].replace(" ", "_")
    return os.path.join(
        birdsongs_dir, row["Date"], species_dir, row["File_Name"])


def build_fields(row):
    """Multipart metadata fields for one detection."""
    return {
        "detection_id": str(row["rowid"]),
        "species_sci": row["Sci_Name"],
        "species_common": row["Com_Name"],
        "detected_at": f'{row["Date"]}T{row["Time"]}',
        "confidence": str(row["Confidence"]),
        "lat": str(row.get("Lat", "")),
        "lon": str(row.get("Lon", "")),
        "audio_filename": row["File_Name"],
    }


def post_detection(session, cfg, row, audio_path):
    """POST one detection. Return True only on a 2xx response."""
    fields = build_fields(row)
    token = f'{cfg["AUTH_SCHEME"]} {cfg["AUTH_TOKEN"]}'.strip()
    headers = {cfg["AUTH_HEADER"]: token} if cfg["AUTH_TOKEN"] else {}
    try:
        if audio_path and os.path.exists(audio_path):
            with open(audio_path, "rb") as fh:
                files = {"audio": (row["File_Name"], fh,
                                   "application/octet-stream")}
                resp = session.post(
                    cfg["ENDPOINT_URL"], data=fields, files=files,
                    headers=headers, timeout=cfg["HTTP_TIMEOUT"])
        else:
            fields = dict(fields, audio_missing="true")
            resp = session.post(
                cfg["ENDPOINT_URL"], data=fields,
                headers=headers, timeout=cfg["HTTP_TIMEOUT"])
        return 200 <= resp.status_code < 300
    except Exception:
        return False


def is_reachable(url, timeout):
    """Cheap TCP connect to the endpoint host:port."""
    from urllib.parse import urlparse
    u = urlparse(url)
    host = u.hostname
    port = u.port or (443 if u.scheme == "https" else 80)
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def drain_once(cfg, session):
    """Forward one batch. Advance the bookmark per success; stop on failure."""
    forwarded = 0
    after = read_bookmark(cfg["STATE_PATH"])
    rows = select_new_detections(cfg["DB_PATH"], after, cfg["BATCH_SIZE"])
    for row in rows:
        if float(row["Confidence"]) < cfg["MIN_CONFIDENCE"]:
            write_bookmark(cfg["STATE_PATH"], row["rowid"])  # filtered out
            continue
        audio = resolve_audio_path(cfg["BIRDSONGS_DIR"], row)
        if post_detection(session, cfg, row, audio):
            write_bookmark(cfg["STATE_PATH"], row["rowid"])
            forwarded += 1
        else:
            break  # retry from this rowid next cycle
    return forwarded


def load_config():
    """Build the runtime config dict from the module constants."""
    return {
        "ENDPOINT_URL": ENDPOINT_URL,
        "AUTH_TOKEN": AUTH_TOKEN,
        "AUTH_HEADER": AUTH_HEADER,
        "AUTH_SCHEME": AUTH_SCHEME,
        "POLL_SECONDS": POLL_SECONDS,
        "BATCH_SIZE": BATCH_SIZE,
        "MIN_CONFIDENCE": MIN_CONFIDENCE,
        "DB_PATH": os.path.expanduser(DB_PATH),
        "BIRDSONGS_DIR": os.path.expanduser(BIRDSONGS_DIR),
        "STATE_PATH": os.path.expanduser(STATE_PATH),
        "HTTP_TIMEOUT": HTTP_TIMEOUT,
    }


def main():
    import requests
    cfg = load_config()
    session = requests.Session()
    print(f"[webhook-forwarder] endpoint={cfg['ENDPOINT_URL']} "
          f"poll={cfg['POLL_SECONDS']}s db={cfg['DB_PATH']}", flush=True)
    while True:
        if is_reachable(cfg["ENDPOINT_URL"], cfg["HTTP_TIMEOUT"]):
            total = 0
            while True:
                n = drain_once(cfg, session)
                total += n
                if n < cfg["BATCH_SIZE"]:
                    break
            if total:
                print(f"[webhook-forwarder] forwarded {total}", flush=True)
        time.sleep(cfg["POLL_SECONDS"])


if __name__ == "__main__":
    main()
