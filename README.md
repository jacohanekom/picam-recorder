# picam-recorder

Records H.264 RTSP streams to MP4 files with configurable pre-roll and post-roll buffering. Designed for Raspberry Pi camera systems and runs as a systemd service.

## Features

- Continuous rolling pre-buffer (default 10 s) — captures footage *before* the record command arrives
- Configurable post-buffer (default 10 s) — keeps recording after the stop command
- TCP control server for start / stop / status / list commands
- Sidecar CSV metadata file per recording (frame number, RTP time, wall-clock time, NAL type)
- Robust RTP-to-wall-clock sync via RTSP Sender Report packets
- Handles both Annex-B and AVCC H.264 packet formats

## Dependencies

```
libavformat-dev  libavcodec-dev  libavutil-dev
g++  pkg-config
```

## Build

```bash
make
```

## Install

```bash
make install
```

Installs to:

| Path | Contents |
|------|----------|
| `/usr/bin/picam-recorder` | Binary |
| `/etc/picam-recorder/recorder.ini` | Config file |
| `/lib/systemd/system/picam-recorder.service` | Systemd unit |
| `/var/lib/picam-recorder/` | Recording output directory |

### Debian package

```bash
dpkg-buildpackage -us -uc -b
sudo dpkg -i ../picam-recorder_*_arm64.deb
```

### From the APT repository

CI publishes to a signed APT repository (shared with other aipicam Raspberry Pi packages) hosted on Cloudflare R2, with two channels:

- **`main`** — pushing a `v*` tag publishes the clean release version here.
- **`nightly`** — every push (to any branch, and PRs) publishes a dev build here, versioned with a `+<UTC timestamp>` suffix.

```bash
curl -fsSL https://apt.aipicam.com/pubkey.asc | sudo gpg --dearmor -o /usr/share/keyrings/aipicam.gpg

# stable releases
echo "deb [signed-by=/usr/share/keyrings/aipicam.gpg] https://apt.aipicam.com main main" | sudo tee /etc/apt/sources.list.d/aipicam.list

# or nightly builds instead
echo "deb [signed-by=/usr/share/keyrings/aipicam.gpg] https://apt.aipicam.com nightly main" | sudo tee /etc/apt/sources.list.d/aipicam.list

sudo apt-get update
sudo apt-get install picam-recorder
```

Builds run on GitHub's native `ubuntu-24.04-arm` hosted runner (no QEMU). Uses the same `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `GPG_PRIVATE_KEY`, and `GPG_KEY_ID` repo secrets described in [pi-block-cpu-cores](../pi-block-cpu-cores)'s README, since it publishes into the same shared repo.

## Configuration

`/etc/picam-recorder/recorder.ini`:

```ini
rtsp = rtsp://127.0.0.1:8554/main   # RTSP stream URL
dir  = /var/lib/picam-recorder      # Output directory
port = 8080                          # TCP control port
pre  = 10                            # Pre-buffer seconds
post = 10                            # Post-buffer seconds
```

All options can be overridden on the command line:

```bash
picam-recorder \
  --config /path/to/recorder.ini \
  --rtsp rtsp://camera.local/stream \
  --dir /var/recordings \
  --port 8080 \
  --pre 15 \
  --post 5
```

## Systemd service

```bash
sudo systemctl enable --now picam-recorder
sudo journalctl -fu picam-recorder
```

The service restarts automatically on failure (5 s delay) and is pinned to CPU core 2.

## Control protocol

Plain-text TCP on port 8080. Send one command per connection; response is `key=value` pairs terminated by a blank line.

| Command | Description |
|---------|-------------|
| `start <name>` | Start recording to `<name>.mp4` |
| `stop` | Stop recording; drains post-buffer before closing |
| `status` | Returns current recording state |
| `list` | Lists all recordings with metadata |

```bash
echo 'start clip01'  | nc 127.0.0.1 8080
echo 'status'        | nc 127.0.0.1 8080
echo 'stop'          | nc 127.0.0.1 8080
echo 'list'          | nc 127.0.0.1 8080
```

## Output files

Each recording produces two files:

- **`<name>.mp4`** — H.264 video in MP4 container
- **`<name>.csv`** — Frame metadata: `frame`, `rtp_time`, `wall_time` (RFC 3339), `nal_type`
