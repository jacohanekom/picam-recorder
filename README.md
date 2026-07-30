# picam-recorder

Records a Motion-JPEG stream to AVI files with configurable pre-roll and post-roll buffering, and continuously publishes that same MJPEG capture live over HTTP. Designed for Raspberry Pi camera systems and runs as a systemd service.

## Features

- Continuous rolling pre-buffer (default 10 s) — captures footage *before* the record command arrives
- Configurable post-buffer (default 10 s) — keeps recording after the stop command
- TCP control server for start / stop / status / list commands
- Sidecar CSV metadata file per recording
- Per-core parallel JPEG capture, muxed straight into AVI as frames arrive — no batch transcode step
- JPEG compression via libjpeg-turbo's TurboJPEG API — same encoder and 1-100 IJG quality scale as picam-orchestrator-go's own live MJPEG tiers
- **`GET /stream?stream=main-high|main-low`** — every captured frame is compressed at two qualities and published as continuous MJPEG (`multipart/x-mixed-replace`), always, independent of whether a recording is in progress. picam-orchestrator-go proxies this straight through to its own live main view instead of separately re-compressing the same frames itself
- `main-low` is also downscaled (default 1/2, both dimensions) before compressing — two full-native-resolution compressions per frame turned out not to be sustainable at 30fps on real hardware; see [Configuration](#configuration)

## Dependencies

```
libavformat-dev  libavcodec-dev  libavutil-dev  libturbojpeg0-dev
g++  pkg-config
```

FFmpeg (libavformat/libavcodec/libavutil) is used only for AVI muxing — JPEG compression itself is libjpeg-turbo.

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
raw_host = 127.0.0.1                 # picam-raw's UDP host
raw_port = 8560                      # picam-raw's UDP port (defaults from /etc/aipicam/streams.conf)
dir  = /var/lib/picam-recorder       # Output directory
port = 8080                          # TCP control port
stream_port = 8081                   # HTTP port for GET /stream (always live, see Features above)
pre  = 10                            # Pre-buffer seconds
post = 10                            # Post-buffer seconds
mjpeg_quality_high = 85              # 1-100 IJG scale (libjpeg-turbo) -- also what recordings use
mjpeg_quality_low  = 40              # 1-100 IJG scale (libjpeg-turbo)
main_low_scale_divisor = 2           # main-low is downscaled by this factor before compressing; 1 = native resolution
```

All options can be overridden on the command line:

```bash
picam-recorder \
  --config /path/to/recorder.ini \
  --raw-host 127.0.0.1 \
  --raw-port 8560 \
  --dir /var/recordings \
  --port 8080 \
  --stream-port 8081 \
  --pre 15 \
  --post 5 \
  --mjpeg-quality-high 85 \
  --mjpeg-quality-low 40 \
  --main-low-scale-divisor 2
```

## Systemd service

```bash
sudo systemctl enable --now picam-recorder
sudo journalctl -fu picam-recorder
```

The service restarts automatically on failure (5 s delay) and is pinned to CPU core 2.

## Streaming

`GET /stream?stream=main-high|main-low` on `stream_port` (default 8081): `multipart/x-mixed-replace` MJPEG, always live — the same frames end up here whether or not `start`/`stop` has been called on the control port.

```bash
curl http://127.0.0.1:8081/stream?stream=main-high -o /dev/null
```

## Control protocol

Plain-text TCP on port 8080. Send one command per connection; response is `key=value` pairs terminated by a blank line.

| Command | Description |
|---------|-------------|
| `start <name>` | Start recording to `<name>.avi` |
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

- **`<name>.avi`** — Motion-JPEG video in an AVI container
- **`<name>.csv`** — Frame metadata: `frame`, `frame_seq`, `ts_us`, `rtp_time`, `wall_time` (RFC 3339), `nal_type`
