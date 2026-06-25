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
| `/usr/local/bin/picam-recorder` | Binary |
| `/etc/picam-recorder/recorder.ini` | Config file |
| `/lib/systemd/system/picam-recorder.service` | Systemd unit |
| `/var/lib/picam-recorder/` | Recording output directory |

### Debian package

```bash
dpkg-buildpackage -us -uc -F
sudo dpkg -i ../picam-recorder_*_arm64.deb
```

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
