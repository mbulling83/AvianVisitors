# Forwarding

Default install hosts the collage at `http://birdnet.local/` on your LAN, no auth. The recipes below are independent. Pick what you need.

---

## 1. Cloudflare Tunnel

Public HTTPS URL, no port forwarding. Needs a free Cloudflare account.

```bash
sudo apt install -y lsb-release
curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg \
  | sudo tee /usr/share/keyrings/cloudflare-main.gpg >/dev/null
echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared $(lsb_release -cs) main" \
  | sudo tee /etc/apt/sources.list.d/cloudflared.list
sudo apt update && sudo apt install -y cloudflared

cloudflared tunnel login
cloudflared tunnel create birds
cloudflared tunnel route dns birds birds.your-domain.com

sudo cp ~/BirdNET-Pi/avian/forwarding/cloudflared.yml /etc/cloudflared/config.yml
# Edit /etc/cloudflared/config.yml: set `tunnel:` to your UUID
sudo cloudflared service install
sudo systemctl restart cloudflared
```

Add a password gate via Cloudflare Access (free for up to 50 users) or via Caddy basic_auth ([`caddy-auth.caddy`](caddy-auth.caddy)).

---

## 2. Home Assistant sensor

Add to `configuration.yaml`:

```yaml
rest:
  - resource: http://birdnet.local/avian/api/birdnet-api.php?action=recent&hours=1
    scan_interval: 60
    sensor:
      - name: "Latest Bird"
        value_template: "{{ value_json.species[0].com if value_json.species else 'none' }}"
        json_attributes_path: "$.species[0]"
        json_attributes:
          - sci
          - n
          - last_seen
          - best_conf
```

---

## 3. MQTT bridge

```bash
sudo pip3 install paho-mqtt --break-system-packages
cp ~/BirdNET-Pi/avian/forwarding/mqtt-bridge.py ~/avian-mqtt.py
# Edit ~/avian-mqtt.py: broker host, topic prefix, credentials
sudo cp ~/BirdNET-Pi/avian/forwarding/avian-mqtt.service /etc/systemd/system/
# Edit /etc/systemd/system/avian-mqtt.service: set User= to your username
sudo systemctl daemon-reload
sudo systemctl enable --now avian-mqtt
```

Polls `birdnet-api.php?action=recent&hours=1` every 60 seconds. Publishes new species under `birdnet/<slug>` as JSON. Dedup is in-memory; restarts re-emit recent detections.

---

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
# Edit /etc/systemd/system/avian-webhook.service: set User= to your username
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
