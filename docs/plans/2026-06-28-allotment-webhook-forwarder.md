# Allotment Webhook Forwarder Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a self-contained forwarder daemon that drains new BirdNET-Pi detections (metadata + audio clip) to a generic HTTPS webhook whenever the Pi regains connectivity, with at-least-once, no-duplicate, resume-mid-backlog delivery.

**Architecture:** A single copyable Python script (`avian/forwarding/webhook-forwarder.py`) modelled on the existing `mqtt-bridge.py`. It reads `birds.db` read-only, tracks a durable high-water-mark (last forwarded `rowid`) in a JSON state file, and POSTs each new detection as `multipart/form-data`. A systemd unit (`avian-webhook.service`) runs it. The script is fully self-contained (no repo imports) so it can be copied to `~` and run standalone, exactly like the MQTT bridge — but it is structured as small pure functions so tests can load it by file path via `importlib` and exercise every branch without a Pi, mic, or real endpoint.

**Tech Stack:** Python 3.11, `requests` (already in `requirements.txt`), stdlib `sqlite3`/`json`/`socket`, `pytest`/`unittest` for tests, systemd for the service.

---

## Reference facts (verified in repo)

- **Detections table** (`scripts/createdb.sh`): columns `Date, Time, Sci_Name, Com_Name, Confidence, Lat, Lon, Cutoff, Week, Sens, Overlap, File_Name`. `rowid` is the implicit monotonic key. DB path on Pi: `~/BirdNET-Pi/scripts/birds.db`.
- **Audio file path** (`scripts/play.php:554`, `avian/api/recording.php:35`):
  `~/BirdSongs/Extracted/By_Date/<Date>/<Com_Name with spaces→underscores>/<File_Name>`.
- **Existing forwarder conventions** (`avian/forwarding/`): self-contained script with inline `CONST = ...` config at top (`mqtt-bridge.py` uses `BROKER`, `PI_URL`, etc.); systemd unit `avian-mqtt.service` with `User=birdnet`, `WorkingDirectory=~`, `Restart=on-failure`, `After=network-online.target`; install recipe documented as a numbered section in `avian/forwarding/README.md`.
- **Test conventions**: `tests/` uses `unittest.TestCase`, run via `pytest`. `tests/helpers.py` holds shared fixtures. Tests are discovered as `tests/test_*.py`.

## Module structure (target shape of `webhook-forwarder.py`)

Pure, individually-testable functions plus a thin `main()` loop:

- `load_config()` → dict from module-level constants (so tests can override).
- `read_bookmark(state_path)` / `write_bookmark(state_path, rowid)` → int / None.
- `select_new_detections(db_path, after_rowid, batch_size)` → list of dict rows incl. `rowid`.
- `resolve_audio_path(birdsongs_dir, row)` → str path (may not exist).
- `build_fields(row)` → dict of multipart form fields.
- `post_detection(session, cfg, row, audio_path)` → bool delivered (True only on 2xx).
- `is_reachable(url, timeout)` → bool.
- `drain_once(cfg, session)` → int count forwarded this pass (advances bookmark per success).
- `main()` → loop: sleep, check reachable, drain, repeat.

Config constants at top: `ENDPOINT_URL`, `AUTH_TOKEN`, `AUTH_HEADER` (default `"Authorization"`), `AUTH_SCHEME` (default `"Bearer"`), `POLL_SECONDS` (30), `BATCH_SIZE` (25), `MIN_CONFIDENCE` (0.0), `DB_PATH` (`~/BirdNET-Pi/scripts/birds.db`), `BIRDSONGS_DIR` (`~/BirdSongs/Extracted/By_Date`), `STATE_PATH` (`~/avian-webhook-state.json`), `HTTP_TIMEOUT` (30).

---

## Task 1: Test scaffolding — load the script by path

**Files:**
- Create: `avian/forwarding/webhook-forwarder.py` (stub so it can be loaded)
- Create: `tests/test_webhook_forwarder.py`

**Step 1: Write the failing test**

```python
# tests/test_webhook_forwarder.py
import importlib.util
import os
import unittest

MODULE_PATH = os.path.join(
    os.path.dirname(__file__), '..', 'avian', 'forwarding', 'webhook-forwarder.py'
)


def load_module():
    spec = importlib.util.spec_from_file_location('webhook_forwarder', MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestModuleLoads(unittest.TestCase):
    def test_exposes_expected_functions(self):
        mod = load_module()
        for name in [
            'read_bookmark', 'write_bookmark', 'select_new_detections',
            'resolve_audio_path', 'build_fields', 'post_detection',
            'is_reachable', 'drain_once', 'main',
        ]:
            self.assertTrue(hasattr(mod, name), f'missing {name}')
```

**Step 2: Run to verify it fails**

Run: `pytest tests/test_webhook_forwarder.py -v`
Expected: FAIL — file missing / functions absent.

**Step 3: Create the stub script**

Create `avian/forwarding/webhook-forwarder.py` with the shebang, docstring, config constants (values from "Module structure" above), and empty `def` stubs (`pass` / `raise NotImplementedError`) for every function listed, plus `if __name__ == "__main__": main()`. Importantly: do NOT run the loop on import (guard with `__main__`).

**Step 4: Run to verify it passes**

Run: `pytest tests/test_webhook_forwarder.py -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add avian/forwarding/webhook-forwarder.py tests/test_webhook_forwarder.py
git commit -m "[TEST] forwarding: scaffold webhook forwarder module + loader test"
```

---

## Task 2: Bookmark read/write (durable high-water-mark)

**Files:**
- Modify: `avian/forwarding/webhook-forwarder.py`
- Test: `tests/test_webhook_forwarder.py`

**Step 1: Write the failing tests**

```python
class TestBookmark(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()
        import tempfile
        self.dir = tempfile.mkdtemp()
        self.state = os.path.join(self.dir, 'state.json')

    def test_missing_file_returns_zero(self):
        self.assertEqual(self.mod.read_bookmark(self.state), 0)

    def test_write_then_read_roundtrip(self):
        self.mod.write_bookmark(self.state, 42)
        self.assertEqual(self.mod.read_bookmark(self.state), 42)

    def test_corrupt_file_returns_zero(self):
        with open(self.state, 'w') as f:
            f.write('not json{')
        self.assertEqual(self.mod.read_bookmark(self.state), 0)
```

**Step 2: Run — expect FAIL** (`NotImplementedError`).
Run: `pytest tests/test_webhook_forwarder.py::TestBookmark -v`

**Step 3: Implement**

```python
def read_bookmark(state_path):
    try:
        with open(state_path) as f:
            return int(json.load(f)["last_forwarded_id"])
    except (FileNotFoundError, ValueError, KeyError, json.JSONDecodeError):
        return 0


def write_bookmark(state_path, rowid):
    tmp = state_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_forwarded_id": int(rowid)}, f)
    os.replace(tmp, state_path)  # atomic
```

**Step 4: Run — expect PASS.**

**Step 5: Commit**

```bash
git add -A && git commit -m "[FEAT] forwarding: durable rowid bookmark read/write"
```

---

## Task 3: Select new detections from birds.db

**Files:** Modify script; Test in `tests/test_webhook_forwarder.py`

**Step 1: Write the failing tests**

```python
class TestSelect(unittest.TestCase):
    def setUp(self):
        import sqlite3, tempfile
        self.mod = load_module()
        self.db = os.path.join(tempfile.mkdtemp(), 'birds.db')
        con = sqlite3.connect(self.db)
        con.execute(
            "CREATE TABLE detections (Date DATE, Time TIME, Sci_Name TEXT, "
            "Com_Name TEXT, Confidence FLOAT, Lat FLOAT, Lon FLOAT, Cutoff FLOAT, "
            "Week INT, Sens FLOAT, Overlap FLOAT, File_Name TEXT)"
        )
        rows = [
            ('2026-06-28', '05:14:22', 'Erithacus rubecula', 'European Robin',
             0.91, 51.4, -0.1, 0.7, 26, 1.25, 0.0, 'robin.wav'),
            ('2026-06-28', '05:15:01', 'Pica pica', 'Eurasian Magpie',
             0.55, 51.4, -0.1, 0.7, 26, 1.25, 0.0, 'magpie.wav'),
            ('2026-06-28', '05:16:00', 'Turdus merula', 'Eurasian Blackbird',
             0.80, 51.4, -0.1, 0.7, 26, 1.25, 0.0, 'blackbird.wav'),
        ]
        con.executemany(
            "INSERT INTO detections VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        con.commit(); con.close()

    def test_selects_all_after_zero(self):
        out = self.mod.select_new_detections(self.db, after_rowid=0, batch_size=25)
        self.assertEqual([r['rowid'] for r in out], [1, 2, 3])
        self.assertEqual(out[0]['Com_Name'], 'European Robin')

    def test_respects_after_rowid(self):
        out = self.mod.select_new_detections(self.db, after_rowid=2, batch_size=25)
        self.assertEqual([r['rowid'] for r in out], [3])

    def test_respects_batch_size_and_order(self):
        out = self.mod.select_new_detections(self.db, after_rowid=0, batch_size=2)
        self.assertEqual([r['rowid'] for r in out], [1, 2])
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement**

```python
def select_new_detections(db_path, after_rowid, batch_size):
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
```

**Step 4: Run — expect PASS.**

**Step 5: Commit** `[FEAT] forwarding: read new detections from birds.db (read-only)`

---

## Task 4: Resolve audio path + build multipart fields

**Files:** Modify script; Test in `tests/test_webhook_forwarder.py`

**Step 1: Write the failing tests**

```python
class TestAudioAndFields(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()
        self.row = {
            'rowid': 7, 'Date': '2026-06-28', 'Time': '05:14:22',
            'Sci_Name': 'Erithacus rubecula', 'Com_Name': 'European Robin',
            'Confidence': 0.91, 'Lat': 51.4, 'Lon': -0.1, 'File_Name': 'robin.wav',
        }

    def test_resolve_audio_path_spaces_to_underscores(self):
        p = self.mod.resolve_audio_path('/base', self.row)
        self.assertEqual(p, '/base/2026-06-28/European_Robin/robin.wav')

    def test_build_fields_contains_idempotency_key(self):
        f = self.mod.build_fields(self.row)
        self.assertEqual(f['detection_id'], '7')
        self.assertEqual(f['species_common'], 'European Robin')
        self.assertEqual(f['species_sci'], 'Erithacus rubecula')
        self.assertEqual(f['detected_at'], '2026-06-28T05:14:22')
        self.assertEqual(f['confidence'], '0.91')
        self.assertEqual(f['audio_filename'], 'robin.wav')
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement**

```python
def resolve_audio_path(birdsongs_dir, row):
    species_dir = row["Com_Name"].replace(" ", "_")
    return os.path.join(
        birdsongs_dir, row["Date"], species_dir, row["File_Name"])


def build_fields(row):
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
```

**Step 4: Run — expect PASS.**

**Step 5: Commit** `[FEAT] forwarding: resolve audio path + build multipart fields`

---

## Task 5: post_detection — multipart POST, audio_missing, 2xx semantics

**Files:** Modify script; Test in `tests/test_webhook_forwarder.py`

Use a fake session object (duck-typed `.post`) so no network is touched.

**Step 1: Write the failing tests**

```python
class FakeResp:
    def __init__(self, status): self.status_code = status

class FakeSession:
    def __init__(self, status=200):
        self.status = status; self.calls = []
    def post(self, url, **kw):
        self.calls.append((url, kw)); return FakeResp(self.status)

class TestPost(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.mod = load_module()
        self.cfg = {
            'ENDPOINT_URL': 'https://example/hook', 'AUTH_TOKEN': 't',
            'AUTH_HEADER': 'Authorization', 'AUTH_SCHEME': 'Bearer',
            'HTTP_TIMEOUT': 5,
        }
        self.row = {'rowid': 1, 'Date': '2026-06-28', 'Time': '05:14:22',
                    'Sci_Name': 'X', 'Com_Name': 'Y', 'Confidence': 0.9,
                    'Lat': 1, 'Lon': 2, 'File_Name': 'a.wav'}
        self.dir = tempfile.mkdtemp()
        self.audio = os.path.join(self.dir, 'a.wav')

    def test_2xx_returns_true_and_sends_auth_header(self):
        with open(self.audio, 'wb') as f: f.write(b'RIFFdata')
        s = FakeSession(200)
        ok = self.mod.post_detection(s, self.cfg, self.row, self.audio)
        self.assertTrue(ok)
        url, kw = s.calls[0]
        self.assertEqual(kw['headers']['Authorization'], 'Bearer t')
        self.assertIn('files', kw)  # audio attached
        self.assertEqual(kw['data']['detection_id'], '1')

    def test_non_2xx_returns_false(self):
        with open(self.audio, 'wb') as f: f.write(b'x')
        s = FakeSession(500)
        self.assertFalse(self.mod.post_detection(s, self.cfg, self.row, self.audio))

    def test_missing_audio_sends_metadata_only_flag(self):
        s = FakeSession(200)
        ok = self.mod.post_detection(s, self.cfg, self.row, self.audio)  # no file
        self.assertTrue(ok)
        url, kw = s.calls[0]
        self.assertEqual(kw['data']['audio_missing'], 'true')
        self.assertNotIn('files', kw)

    def test_network_exception_returns_false(self):
        class Boom:
            def post(self, *a, **k): raise OSError('down')
        with open(self.audio, 'wb') as f: f.write(b'x')
        self.assertFalse(self.mod.post_detection(Boom(), self.cfg, self.row, self.audio))
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement**

```python
def post_detection(session, cfg, row, audio_path):
    fields = build_fields(row)
    headers = {cfg["AUTH_HEADER"]: f'{cfg["AUTH_SCHEME"]} {cfg["AUTH_TOKEN"]}'.strip()}
    try:
        if audio_path and os.path.exists(audio_path):
            with open(audio_path, "rb") as fh:
                files = {"audio": (row["File_Name"], fh, "application/octet-stream")}
                resp = session.post(cfg["ENDPOINT_URL"], data=fields, files=files,
                                    headers=headers, timeout=cfg["HTTP_TIMEOUT"])
        else:
            fields = dict(fields, audio_missing="true")
            resp = session.post(cfg["ENDPOINT_URL"], data=fields,
                                headers=headers, timeout=cfg["HTTP_TIMEOUT"])
        return 200 <= resp.status_code < 300
    except Exception:
        return False
```

**Step 4: Run — expect PASS.**

**Step 5: Commit** `[FEAT] forwarding: multipart POST with audio + audio_missing fallback`

---

## Task 6: drain_once — advance bookmark per success, stop on failure, MIN_CONFIDENCE

**Files:** Modify script; Test in `tests/test_webhook_forwarder.py`

`drain_once` ties select + post + bookmark together. It must: advance the bookmark only after each successful POST; stop the pass on the first failure (so the next item retries next cycle from the same bookmark); skip rows below `MIN_CONFIDENCE` by advancing past them WITHOUT posting (they are permanently filtered, bookmark moves on).

**Step 1: Write the failing tests**

```python
class TestDrain(unittest.TestCase):
    def setUp(self):
        import sqlite3, tempfile
        self.mod = load_module()
        d = tempfile.mkdtemp()
        self.db = os.path.join(d, 'birds.db')
        self.state = os.path.join(d, 'state.json')
        self.birdsongs = os.path.join(d, 'By_Date')
        con = sqlite3.connect(self.db)
        con.execute(
            "CREATE TABLE detections (Date,Time,Sci_Name,Com_Name,Confidence,"
            "Lat,Lon,Cutoff,Week,Sens,Overlap,File_Name)")
        con.executemany(
            "INSERT INTO detections VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
              ('2026-06-28','05:14','A','Robin',0.91,1,2,0.7,26,1.25,0.0,'a.wav'),
              ('2026-06-28','05:15','B','Magpie',0.40,1,2,0.7,26,1.25,0.0,'b.wav'),
              ('2026-06-28','05:16','C','Crow',0.80,1,2,0.7,26,1.25,0.0,'c.wav'),
            ])
        con.commit(); con.close()
        self.cfg = {
            'DB_PATH': self.db, 'STATE_PATH': self.state,
            'BIRDSONGS_DIR': self.birdsongs, 'BATCH_SIZE': 25, 'MIN_CONFIDENCE': 0.0,
            'ENDPOINT_URL': 'u', 'AUTH_TOKEN': 't', 'AUTH_HEADER': 'Authorization',
            'AUTH_SCHEME': 'Bearer', 'HTTP_TIMEOUT': 5,
        }

    def test_all_forwarded_advances_bookmark_to_last(self):
        s = FakeSession(200)
        n = self.mod.drain_once(self.cfg, s)
        self.assertEqual(n, 3)
        self.assertEqual(self.mod.read_bookmark(self.state), 3)

    def test_failure_midway_stops_and_keeps_bookmark_at_last_success(self):
        # Succeed on rowid 1, fail on rowid 2
        class FlakySession:
            def __init__(s): s.calls = 0
            def post(s, *a, **k):
                s.calls += 1
                return FakeResp(200 if s.calls == 1 else 500)
        self.mod.drain_once(self.cfg, FlakySession())
        self.assertEqual(self.mod.read_bookmark(self.state), 1)

    def test_min_confidence_skips_without_post_but_advances(self):
        self.cfg['MIN_CONFIDENCE'] = 0.7
        s = FakeSession(200)
        self.mod.drain_once(self.cfg, s)
        # Robin(0.91) + Crow(0.80) posted, Magpie(0.40) skipped; bookmark at 3
        self.assertEqual(len(s.calls), 2)
        self.assertEqual(self.mod.read_bookmark(self.state), 3)
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement**

```python
def drain_once(cfg, session):
    forwarded = 0
    after = read_bookmark(cfg["STATE_PATH"])
    rows = select_new_detections(cfg["DB_PATH"], after, cfg["BATCH_SIZE"])
    for row in rows:
        if float(row["Confidence"]) < cfg["MIN_CONFIDENCE"]:
            write_bookmark(cfg["STATE_PATH"], row["rowid"])  # filtered, skip
            continue
        audio = resolve_audio_path(cfg["BIRDSONGS_DIR"], row)
        if post_detection(session, cfg, row, audio):
            write_bookmark(cfg["STATE_PATH"], row["rowid"])
            forwarded += 1
        else:
            break  # retry from this rowid next cycle
    return forwarded
```

**Step 4: Run — expect PASS.**

**Step 5: Commit** `[FEAT] forwarding: drain pass with per-success bookmark + confidence filter`

---

## Task 7: is_reachable + main loop wiring

**Files:** Modify script; Test in `tests/test_webhook_forwarder.py`

`is_reachable` does a cheap TCP connect to the endpoint host:port (no full HTTP). `main()` builds cfg from the module constants, opens a `requests.Session()`, then loops: if reachable → `drain_once` until it returns 0 (or batch < BATCH_SIZE), else sleep. Keep `main()` thin; test only the cfg-builder and reachability — do NOT loop forever in a test.

**Step 1: Write the failing tests**

```python
class TestReachableAndConfig(unittest.TestCase):
    def setUp(self): self.mod = load_module()

    def test_is_reachable_false_for_unroutable_quickly(self):
        # 203.0.113.0/24 is TEST-NET-3, guaranteed unroutable
        self.assertFalse(
            self.mod.is_reachable('https://203.0.113.1/hook', timeout=1))

    def test_load_config_reads_module_constants(self):
        cfg = self.mod.load_config()
        self.assertIn('ENDPOINT_URL', cfg)
        self.assertIn('POLL_SECONDS', cfg)
        self.assertEqual(cfg['AUTH_HEADER'], 'Authorization')
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement**

```python
def is_reachable(url, timeout):
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


def load_config():
    return {
        "ENDPOINT_URL": ENDPOINT_URL, "AUTH_TOKEN": AUTH_TOKEN,
        "AUTH_HEADER": AUTH_HEADER, "AUTH_SCHEME": AUTH_SCHEME,
        "POLL_SECONDS": POLL_SECONDS, "BATCH_SIZE": BATCH_SIZE,
        "MIN_CONFIDENCE": MIN_CONFIDENCE, "DB_PATH": os.path.expanduser(DB_PATH),
        "BIRDSONGS_DIR": os.path.expanduser(BIRDSONGS_DIR),
        "STATE_PATH": os.path.expanduser(STATE_PATH), "HTTP_TIMEOUT": HTTP_TIMEOUT,
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
```

Ensure imports at top: `import json, os, socket, time, sqlite3`. (`requests` imported lazily in `main` so tests never need it.)

**Step 4: Run — expect PASS.** Run the whole file: `pytest tests/test_webhook_forwarder.py -v`

**Step 5: Commit** `[FEAT] forwarding: reachability check + main drain loop`

---

## Task 8: systemd unit

**Files:**
- Create: `avian/forwarding/avian-webhook.service`

**Step 1: Write the unit** (near-copy of `avian-mqtt.service`)

```ini
[Unit]
Description=AvianVisitors webhook forwarder
After=network-online.target

[Service]
Type=simple
# Edit User= if BirdNET-Pi was installed under a different account.
# ExecStart uses a bare filename; WorkingDirectory=~ respects User=,
# whereas systemd's %h expands to /root for system-mode units.
User=birdnet
WorkingDirectory=~
ExecStart=/usr/bin/python3 avian-webhook.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Step 2: Verify** there is no test to run; sanity-check by eye against `avian-mqtt.service`.

**Step 3: Commit** `[FEAT] forwarding: systemd unit for webhook forwarder`

---

## Task 9: README recipe

**Files:**
- Modify: `avian/forwarding/README.md` (add a new numbered section after "MQTT bridge")

**Step 1: Write the section**

````markdown
## 4. Webhook forwarder (offline-first)

For a Pi running headless somewhere with only intermittent connectivity (e.g. an
allotment on a phone hotspot). Detections accumulate in `birds.db` while offline;
when the Pi rejoins the network, each new detection is POSTed to your HTTPS
endpoint as `multipart/form-data` (metadata fields + the audio clip), exactly
once, resuming cleanly if the link drops mid-backlog.

```bash
sudo pip3 install requests --break-system-packages   # usually already present
cp ~/BirdNET-Pi/avian/forwarding/webhook-forwarder.py ~/avian-webhook.py
# Edit ~/avian-webhook.py: set ENDPOINT_URL and AUTH_TOKEN (and, if you like,
# MIN_CONFIDENCE to skip low-confidence clips and save phone data).
sudo cp ~/BirdNET-Pi/avian/forwarding/avian-webhook.service /etc/systemd/system/
# Edit the unit: set User= to your username.
sudo systemctl daemon-reload
sudo systemctl enable --now avian-webhook
journalctl -u avian-webhook -f   # watch it drain when the hotspot comes up
```

**Connectivity:** save your phone's hotspot as a known WiFi network on the Pi
(e.g. via `nmcli` or a second `network=` block in `wpa_supplicant.conf`, at a
lower priority than home WiFi). On a visit, switch the hotspot on — the Pi
auto-joins in ~30s and the forwarder drains the backlog. No reboot needed.

**Receiver contract:** respond `2xx` only once you've durably stored the
detection. Anything else (or a timeout) means "not delivered" and that exact
detection — keyed by the unique `detection_id` form field — is retried next
cycle. Dedupe on `detection_id` to be safe against retried-but-already-stored
deliveries.

**Form fields:** `detection_id`, `species_sci`, `species_common`, `detected_at`
(ISO-8601), `confidence`, `lat`, `lon`, `audio_filename`, and the `audio` file
part (or `audio_missing=true` if the clip was already purged).
````

**Step 2: Commit** `[DOCS] forwarding: document the webhook forwarder recipe`

---

## Task 10: Full suite + flake8 gate

**Step 1:** Run the whole project test suite to confirm nothing regressed.
Run: `pytest tests/ -v`
Expected: all pass (existing + new `test_webhook_forwarder.py`).

**Step 2:** Lint the new script to match CI (`.github/workflows/python-app.yml` runs flake8; repo has `.flake8`).
Run: `flake8 avian/forwarding/webhook-forwarder.py tests/test_webhook_forwarder.py`
Expected: no errors (fix any line-length/import issues).

**Step 3: Commit** any lint fixes: `[STYLE] forwarding: satisfy flake8`

---

## Done criteria

- `pytest tests/` green, including all `TestBookmark/Select/AudioAndFields/Post/Drain/ReachableAndConfig` cases.
- `flake8` clean on the new files.
- Core guarantee proven by tests: a mid-backlog failure leaves the bookmark at the last success (no dup, no gap), and the next pass resumes from there.

## Out of scope (documented next steps)

- Battery duty-cycle: gate BirdNET analysis to dawn/dusk windows via cron to extend power-bank runtime. Separate change.
- Phone-hotspot auto-join is operator config (`nmcli`/`wpa_supplicant`), documented in the README, not code in this repo.
