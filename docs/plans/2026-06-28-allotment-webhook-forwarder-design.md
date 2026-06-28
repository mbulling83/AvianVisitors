# Allotment Webhook Forwarder — Design

**Date:** 2026-06-28
**Status:** Designed, ready for implementation

## Problem

Run AvianVisitors (BirdNET-Pi fork) headless at an allotment with no fixed
internet. The Pi runs continuously off a USB power bank, detecting birds and
logging every detection (species, time, audio clip, spectrogram) locally. On
occasional visits the owner brings up a phone hotspot and wants the accumulated
detections — metadata **and** the audio recordings — pushed out to a webhook.

### Constraints established during brainstorming

- **Power:** USB battery, always-on. Battery life is the binding constraint.
  The dominant load is continuous BirdNET inference (several watts), not WiFi.
  An idle/scanning WiFi radio is a rounding error (~tens of mW), so leaving a
  saved hotspot to auto-join costs effectively nothing — no restart needed.
- **Connectivity:** intermittent. Pi is offline most of the time; the hotspot
  appears only during visits.
- **Payload:** metadata **+ audio clip** per detection (owner accepts the phone
  data cost of a backlog).
- **Destination:** a generic HTTPS endpoint (n8n-compatible later), configurable
  URL + header token. No n8n-specific assumptions.

## Architecture

Nothing in the detection pipeline changes. BirdNET-Pi already writes every
detection to SQLite (`birds.db`) plus an audio clip + spectrogram on disk — this
is the offline buffer and source of truth.

We add **one new component**: a forwarder daemon (systemd service) living
alongside the existing MQTT bridge in `avian/forwarding/`.

### Connectivity model

The Pi keeps the phone's hotspot saved as a known WiFi network (lower priority
than any home network). On a visit, the owner switches the hotspot on; the Pi
auto-joins in ~30s; the forwarder detects reachability and drains the backlog.
No reboot, no display.

> Rejected alternative: power-cycling the Pi to bring up WiFi (the owner's
> first idea). It saves no meaningful power (WiFi is negligible vs detection),
> adds ~60–90s boot time per visit, and adds SD-corruption risk on each cycle.

## Forwarder daemon

`avian/forwarding/webhook-forwarder.py` — runs continuously, cheaply.

### Loop

- Every `POLL_SECONDS` (~30s), run a lightweight reachability check against the
  endpoint host. Not a constant scan.
- **Offline:** sleep, do nothing — near-zero CPU/power.
- **Online:** enter drain mode.

### Drain mode

1. Read `last_forwarded_id` from the state file (`STATE_PATH`). First run = 0.
2. `SELECT rowid, * FROM detections WHERE rowid > ? ORDER BY rowid ASC
   LIMIT BATCH_SIZE`. Append-only, monotonic rowid ⇒ this is exactly "new since
   last sync".
3. Optionally drop rows below `MIN_CONFIDENCE`.
4. For each row: locate its audio file (DB `File_Name`), build the multipart
   POST, send.
5. **On 2xx:** advance `last_forwarded_id` to that rowid (the durable bookmark),
   *after* the success.
6. **On non-2xx / timeout / connection error:** stop draining, log, sleep,
   retry from the bookmark next cycle.

### Key guarantees

- **rowid as bookmark** ⇒ no duplicates, no missed rows, survives reboots,
  resumes mid-backlog cleanly. A dropped hotspot loses at most the in-flight
  item, which is retried.
- **At-least-once delivery with a dedup key.** The receiver dedupes on
  `detection_id` (the rowid) if a 2xx is lost on the wire and we resend.
- **Missing audio file** (purged by BirdNET-Pi's disk cleanup before forwarding):
  POST metadata only with `audio_missing: true` and advance — never block the
  queue on a deleted clip.

### Why read the DB directly (vs the PHP API like the MQTT bridge)

The MQTT bridge reads `birdnet-api.php?action=recent&hours=1` with in-memory
dedup that re-emits on restart. That is wrong for the offline/backlog case: a
restart would re-send, and the API only returns recent hours, so a multi-day
backlog would be lost. Reading `birds.db` read-only gives (a) the durable
monotonic rowid bookmark and (b) the exact audio file path per detection.

## Payload

One `POST` per detection, `multipart/form-data`, streaming the audio file from
disk (no base64 bloat).

**Auth:** `Authorization: Bearer <token>` (header name configurable).

**Metadata form fields:**

| Field | Example | Source (DB) |
|---|---|---|
| `species_sci` | `Erithacus rubecula` | `Sci_Name` |
| `species_common` | `European Robin` | `Com_Name` |
| `detected_at` | `2026-06-28T05:14:22+01:00` | `Date` + `Time`, ISO-8601 w/ Pi tz |
| `confidence` | `0.91` | `Confidence` |
| `lat` / `lon` | `51.4` / `-0.1` | BirdNET-Pi config |
| `detection_id` | `4821` | rowid — idempotency key |
| `audio_filename` | `Robin-91-2026-...wav` | `File_Name` |
| `audio_missing` | `true` (only when clip gone) | — |

**File part:** `audio` — the clip (`audio/wav` or `audio/mpeg`).

**Receiver contract:** respond 2xx once the detection is durably stored. Any
other status / timeout / dropped connection = "not delivered"; that exact
detection is retried next cycle. Keep the handler fast — a slow receiver slows
the drain but never loses data.

## Config

Inline constants at the top of `webhook-forwarder.py` (matching
`mqtt-bridge.py`'s `BROKER`/`PI_URL` style — no new config system):

`ENDPOINT_URL`, `AUTH_TOKEN`, `AUTH_HEADER`, `POLL_SECONDS`, `BATCH_SIZE`,
`MIN_CONFIDENCE`, `DB_PATH`, `STATE_PATH`.

## Repo layout & install

New files under `avian/forwarding/`:

- `webhook-forwarder.py` — the daemon.
- `avian-webhook.service` — systemd unit, near-copy of `avian-mqtt.service`
  (`User=birdnet`, `WorkingDirectory=~`, `Restart=on-failure`,
  `After=network-online.target`).
- New "Webhook forwarder" section in `forwarding/README.md`, same recipe style.

Install recipe (mirrors the MQTT bridge):

```bash
cp ~/BirdNET-Pi/avian/forwarding/webhook-forwarder.py ~/avian-webhook.py
# Edit ~/avian-webhook.py: ENDPOINT_URL, AUTH_TOKEN, poll, batch, min_confidence
sudo cp ~/BirdNET-Pi/avian/forwarding/avian-webhook.service /etc/systemd/system/
# Edit unit: set User= to your username
sudo systemctl daemon-reload
sudo systemctl enable --now avian-webhook
```

## Testing

Logic is pure and testable without a Pi, mic, or real endpoint. Use the existing
`tests/` dir.

Unit tests against a temp SQLite DB seeded with fake detections + dummy audio:

- new detections selected in rowid order, batched correctly
- bookmark advances only on 2xx; a mid-batch failure leaves the bookmark at the
  last success (resume without dup / without gap — the core guarantee)
- missing audio file → metadata-only POST with `audio_missing`, queue not blocked
- `MIN_CONFIDENCE` filter drops low-confidence rows
- offline (reachability fails) → no POSTs, no bookmark change

Webhook mocked with a local stub recording requests; assert multipart fields +
file bytes match the DB row.

Manual end-to-end on the Pi: point `ENDPOINT_URL` at a throwaway receiver
(webhook.site / local n8n), toggle the hotspot, confirm a seeded backlog drains
and survives a mid-drain hotspot drop.

## Optional next step (NOT in v1): battery duty-cycle

Since battery is the real constraint and detection is the load, a follow-up
could gate BirdNET's recording/analysis to chosen windows (e.g. dawn
04:30–09:00 + dusk) via cron start/stop of the analysis service, potentially
multiplying runtime. Out of scope for this build; documented here so it is not
forgotten.
