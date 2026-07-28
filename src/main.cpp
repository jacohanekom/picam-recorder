/**
 * picam_recorder — records raw YUV420 frames from picam-raw UDP stream to AVI
 *
 * Single-phase pipeline: each raw YUV420 frame is JPEG-compressed --
 * fast, no inter-frame dependencies -- and either kept in a rolling
 * pre-buffer (no recording active) or written directly into the current
 * recording's open AVI/MJPEG file. One core's worth of JPEG encoding
 * can't sustain 30fps at typical recording resolutions, so capture runs
 * one independent JPEG encoder per online CPU core in parallel, with
 * their out-of-order output reassembled back into sequence before it
 * reaches the pre-buffer or the AVI writer. Since a JPEG frame already
 * IS its own final stored format, there's no decode/re-encode/mux step
 * to defer until later the way live H.264 (this project's original
 * design) or a JPEG-then-H.264 batch transcode (an intermediate design
 * from earlier in this project's history) both needed -- closing a
 * recording is just writing the AVI trailer/index, fast and synchronous.
 *
 * Dependencies:
 *   - FFmpeg libs: libavformat, libavcodec, libavutil
 *   No other external dependencies.
 *
 * Build example (Linux / macOS):
 *   g++ -std=c++17 -O2 -pthread \
 *       main.cpp \
 *       $(pkg-config --cflags --libs libavformat libavcodec libavutil) \
 *       -o picam-recorder
 *
 * Usage:
 *   ./picam-recorder [--config recorder.ini] [--raw-host <host>] [--raw-port <n>]
 *                    [--raw-width <n>] [--raw-height <n>] [--raw-fps <n>]
 *                    [--raw-stride <n>] [--port <n>] [--pre <s>] [--post <s>]
 *
 * Control — TCP plain-text protocol (one command per line):
 *
 *   Commands:
 *     start <filename.avi>
 *     stop
 *     status
 *     list
 *
 *   Each response is one or more  key=value  lines followed by a blank line.
 *
 *   Quick test with nc:
 *     echo 'start clip01.avi' | nc 127.0.0.1 8080
 *     echo 'stop'             | nc 127.0.0.1 8080
 *     echo 'status'           | nc 127.0.0.1 8080
 *     echo 'list'             | nc 127.0.0.1 8080
 */

// ── POSIX sockets ─────────────────────────────────────────────────────────────
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <pthread.h>

// ── FFmpeg ────────────────────────────────────────────────────────────────────
extern "C" {
#include <libavformat/avformat.h>
#include <libavcodec/avcodec.h>
#include <libavutil/avutil.h>
#include <libavutil/imgutils.h>
#include <libavutil/time.h>
}

// ── stdlib ────────────────────────────────────────────────────────────────────
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using Clock  = std::chrono::system_clock;
using TP     = Clock::time_point;

// ─────────────────────────────────────────────────────────────────────────────
// RAII wrappers for FFmpeg's C-API alloc/free pairs -- used for the
// persistent per-capture-worker JPEG encoder, so none of that state
// needs manual cleanup at every throw/return point.
// ─────────────────────────────────────────────────────────────────────────────
struct AVCodecContextDeleter { void operator()(AVCodecContext* p) const { if (p) avcodec_free_context(&p); } };
struct AVFrameDeleter        { void operator()(AVFrame* p)        const { if (p) av_frame_free(&p); } };
struct AVPacketDeleter       { void operator()(AVPacket* p)       const { if (p) av_packet_free(&p); } };
using AVCodecContextPtr = std::unique_ptr<AVCodecContext, AVCodecContextDeleter>;
using AVFramePtr        = std::unique_ptr<AVFrame, AVFrameDeleter>;
using AVPacketPtr       = std::unique_ptr<AVPacket, AVPacketDeleter>;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────
static constexpr int    kDefaultPort     = 8080;
static constexpr double kDefaultPreSecs  = 10.0;
static constexpr double kDefaultPostSecs = 10.0;

// The CPU core main() pins the whole process to at startup (see its
// "Pin this process to CPU core 2" step) and that the systemd unit's
// own CPUAffinity= setting matches. Every thread inherits this
// core-only mask by default; the UDP receive and TCP control threads
// are meant to stay confined to it so they always get scheduled
// promptly, uncontended by anything else this process does. Any
// thread that needs real parallelism (JPEG capture workers, H.264
// transcode) must widen to every OTHER core instead of every core --
// sharing this one with them would starve the receive loop of the CPU
// time it needs to keep draining the UDP socket in time, which is
// exactly the stall this whole pipeline exists to avoid (see "[raw]
// Dropped incomplete frame").
static constexpr int kReservedCore = 2;

// Number of CPUs actually online right now (never below 1) -- used to
// size the JPEG capture worker pool and CPU-affinity masks.
static long onlineCpuCount()
{
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n < 1 ? 1 : n;
}

// ─────────────────────────────────────────────────────────────────────────────
// Config  — INI / key=value file + CLI override
// ─────────────────────────────────────────────────────────────────────────────
struct Config {
    std::string rawHost   = "127.0.0.1";
    int         rawPort   = 8560;
    int         rawWidth  = 2304;
    int         rawHeight = 1296;
    int         rawFps    = 30;
    int         rawStride = 0;      // 0 = same as rawWidth
    std::string dir       = "recordings";
    int         port      = kDefaultPort;
    double      preSecs   = kDefaultPreSecs;
    double      postSecs  = kDefaultPostSecs;
};

// Applies /etc/aipicam/streams.conf's [stream] main_width/main_height/
// main_port to cfg as a baseline — picam-raw's wire protocol carries no
// width/height field, so this process's view of the main stream's
// geometry/port must already agree with what picam-raw actually sends.
// aipicam-config is that single shared source of truth instead of this
// file's own possibly-stale copy. Called before recorder.ini is parsed,
// so recorder.ini's own explicit raw_width/raw_height/raw_port (if any)
// still take precedence, same as CLI flags take precedence over both.
// Silently does nothing if the shared file isn't installed.
static void applySharedStreamDefaults(Config& cfg)
{
    std::ifstream f("/etc/aipicam/streams.conf");
    if (!f) return;

    auto trim = [](std::string s) -> std::string {
        auto b = s.find_first_not_of(" \t\r\n");
        if (b == std::string::npos) return {};
        auto e = s.find_last_not_of(" \t\r\n");
        return s.substr(b, e - b + 1);
    };

    std::string line, section;
    while (std::getline(f, line)) {
        auto hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);
        line = trim(line);
        if (line.empty()) continue;
        if (line.front() == '[' && line.back() == ']') {
            section = trim(line.substr(1, line.size() - 2));
            continue;
        }
        if (section != "stream") continue;
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = trim(line.substr(0, eq));
        std::string val = trim(line.substr(eq + 1));
        if (key.empty() || val.empty()) continue;

        if      (key == "main_width")  cfg.rawWidth  = std::stoi(val);
        else if (key == "main_height") cfg.rawHeight = std::stoi(val);
        else if (key == "main_port")   cfg.rawPort   = std::stoi(val);
    }
}

static Config loadIni(const std::string& path)
{
    std::ifstream f(path);
    if (!f)
        throw std::runtime_error("cannot open config file: " + path);

    Config cfg;
    applySharedStreamDefaults(cfg);
    std::string line;
    int lineNo = 0;

    auto trim = [](std::string s) -> std::string {
        auto b = s.find_first_not_of(" \t\r\n");
        if (b == std::string::npos) return {};
        auto e = s.find_last_not_of(" \t\r\n");
        return s.substr(b, e - b + 1);
    };

    auto stripComment = [&](std::string s) -> std::string {
        for (size_t i = 0; i < s.size(); ++i) {
            if (s[i] == '"') { ++i; while (i < s.size() && s[i] != '"') ++i; continue; }
            if (s[i] == '#' || s[i] == ';') return trim(s.substr(0, i));
        }
        return s;
    };

    while (std::getline(f, line)) {
        ++lineNo;
        std::string t = trim(line);
        if (t.empty() || t[0] == '#' || t[0] == ';') continue;

        auto eq = t.find('=');
        if (eq == std::string::npos) {
            std::cerr << "[cfg] " << path << ":" << lineNo << ": skipping line (no '=')\n";
            continue;
        }

        std::string key = trim(t.substr(0, eq));
        std::string val = trim(stripComment(trim(t.substr(eq + 1))));
        if (val.size() >= 2 && val.front() == '"' && val.back() == '"')
            val = val.substr(1, val.size() - 2);

        if      (key == "raw_host")   cfg.rawHost   = val;
        else if (key == "raw_port")   cfg.rawPort   = std::stoi(val);
        else if (key == "raw_width")  cfg.rawWidth  = std::stoi(val);
        else if (key == "raw_height") cfg.rawHeight = std::stoi(val);
        else if (key == "raw_fps")    cfg.rawFps    = std::stoi(val);
        else if (key == "raw_stride") cfg.rawStride = std::stoi(val);
        else if (key == "dir")        cfg.dir       = val;
        else if (key == "port")       cfg.port      = std::stoi(val);
        else if (key == "pre")        cfg.preSecs   = std::stod(val);
        else if (key == "post")       cfg.postSecs  = std::stod(val);
        else
            std::cerr << "[cfg] " << path << ":" << lineNo << ": unknown key '" << key << "'\n";
    }
    return cfg;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

static std::string toRFC3339(TP tp)
{
    auto tt = Clock::to_time_t(tp);
    auto ns  = std::chrono::duration_cast<std::chrono::nanoseconds>(
                   tp.time_since_epoch()).count() % 1'000'000'000LL;
    std::tm tm{};
    gmtime_r(&tt, &tm);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm);
    char out[80];
    std::snprintf(out, sizeof(out), "%s.%09lldZ", buf, (long long)ns);
    return out;
}

static double fileSizeMiB(const std::string& path)
{
    std::error_code ec;
    auto sz = fs::file_size(path, ec);
    return ec ? 0.0 : static_cast<double>(sz) / 1024.0 / 1024.0;
}

// ─────────────────────────────────────────────────────────────────────────────
// BufferedJPEG
// ─────────────────────────────────────────────────────────────────────────────
struct BufferedJPEG {
    std::vector<uint8_t> jpeg;
    int64_t               timestampUs;
    TP                     wallTime;
    uint32_t               frameSeq;
};

// ─────────────────────────────────────────────────────────────────────────────
// RollingBuffer -- holds JPEG-compressed frames now rather than already
// H.264-encoded NALUs, since capture no longer runs a live H.264
// encoder at all (see the file's top comment). Otherwise unchanged:
// same rolling-window push/drain shape.
// ─────────────────────────────────────────────────────────────────────────────
class RollingBuffer {
public:
    explicit RollingBuffer(double secs) : secs_(secs) {}

    void push(std::vector<uint8_t> jpeg, int64_t timestampUs, TP wallTime, uint32_t frameSeq)
    {
        std::lock_guard<std::mutex> lk(mu_);
        frames_.push_back({ std::move(jpeg), timestampUs, wallTime, frameSeq });
        auto cutoff = Clock::now() -
                      std::chrono::duration_cast<Clock::duration>(
                          std::chrono::duration<double>(secs_));
        while (!frames_.empty() && frames_.front().wallTime < cutoff)
            frames_.pop_front();
    }

    std::deque<BufferedJPEG> drain()
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::deque<BufferedJPEG> out;
        std::swap(out, frames_);
        return out;
    }

private:
    std::mutex               mu_;
    std::deque<BufferedJPEG> frames_;
    double                   secs_;
};

// ─────────────────────────────────────────────────────────────────────────────
// MetaWriter  — per-recording CSV sidecar  (<name>.csv)
//
// Columns: frame, rtp_time, wall_time, nal_type
// ─────────────────────────────────────────────────────────────────────────────
class MetaWriter {
public:
    explicit MetaWriter(const std::string& mp4Path)
    {
        fs::path p(mp4Path);
        path_ = p.replace_extension(".csv").string();
    }

    void record(int nalType, uint32_t dts, TP wallTime, uint32_t frameSeq)
    {
        int64_t tsUs = std::chrono::duration_cast<std::chrono::microseconds>(
                           wallTime.time_since_epoch()).count();
        std::lock_guard<std::mutex> lk(mu_);
        ++count_;
        rows_.push_back({ count_, dts, tsUs, toRFC3339(wallTime), nalType, frameSeq });
    }

    bool close()
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::ofstream f(path_);
        if (!f) return false;
        f << "frame,frame_seq,ts_us,rtp_time,wall_time,nal_type\n";
        for (auto& r : rows_)
            f << r.frame << ',' << r.frameSeq << ',' << r.tsUs << ','
              << r.dts << ',' << r.wallTime << ',' << r.nalType << '\n';
        return true;
    }

private:
    struct Row { int frame; uint32_t dts; int64_t tsUs; std::string wallTime; int nalType; uint32_t frameSeq; };

    std::mutex       mu_;
    std::string      path_;
    std::vector<Row> rows_;
    int              count_ = 0;
};

// ─────────────────────────────────────────────────────────────────────────────
// AviWriter — muxes MJPEG frames straight into an AVI file
//
// A JPEG-compressed frame needs no decoding, re-encoding, or pixel-
// format conversion to become the final stored format -- it already
// IS an MJPEG video frame -- so unlike an H.264 muxer there's no
// encoder state, no SPS/PPS/extradata to build, and no per-frame
// timestamp accumulation from an external clock: PTS is just a plain
// incrementing frame count in the stream's own {1, fps} timebase,
// always monotonic even across any frames FrameQueue had to drop
// under sustained backpressure. Same libavformat idioms as this file
// used for MP4 muxing before it: pkt_->data borrows the caller's
// buffer directly for the duration of one av_interleaved_write_frame
// call, same as every packet write in this codebase already does.
// ─────────────────────────────────────────────────────────────────────────────
class AviWriter {
public:
    AviWriter(const std::string& path, int width, int height, int fps)
        : meta_(path)
    {
        if (avformat_alloc_output_context2(&fmtCtx_, nullptr, "avi", path.c_str()) < 0 || !fmtCtx_)
            throw std::runtime_error("avformat_alloc_output_context2 (avi) failed");

        stream_ = avformat_new_stream(fmtCtx_, nullptr);
        if (!stream_) throw std::runtime_error("avformat_new_stream failed");

        const int useFps = fps > 0 ? fps : 30;
        stream_->codecpar->codec_type = AVMEDIA_TYPE_VIDEO;
        stream_->codecpar->codec_id   = AV_CODEC_ID_MJPEG;
        stream_->codecpar->format     = AV_PIX_FMT_YUVJ420P;
        stream_->codecpar->width      = width;
        stream_->codecpar->height     = height;
        stream_->time_base            = { 1, useFps };
        stream_->avg_frame_rate       = { useFps, 1 };

        if (avio_open(&fmtCtx_->pb, path.c_str(), AVIO_FLAG_WRITE) < 0)
            throw std::runtime_error("avio_open failed for: " + path);

        if (avformat_write_header(fmtCtx_, nullptr) < 0)
            throw std::runtime_error("avformat_write_header failed");

        pkt_ = av_packet_alloc();
        if (!pkt_) throw std::runtime_error("av_packet_alloc failed");
    }

    ~AviWriter()
    {
        if (pkt_)    av_packet_free(&pkt_);
        if (fmtCtx_) avformat_free_context(fmtCtx_);
    }

    AviWriter(const AviWriter&)            = delete;
    AviWriter& operator=(const AviWriter&) = delete;

    void writeFrame(const std::vector<uint8_t>& jpeg, int64_t timestampUs, uint32_t frameSeq)
    {
        std::lock_guard<std::mutex> lk(mu_);

        av_packet_unref(pkt_);
        pkt_->data         = const_cast<uint8_t*>(jpeg.data());
        pkt_->size         = static_cast<int>(jpeg.size());
        pkt_->stream_index = stream_->index;
        pkt_->pts          = frameCount_;
        pkt_->dts          = frameCount_;
        pkt_->duration     = 1;
        pkt_->flags       |= AV_PKT_FLAG_KEY; // every MJPEG frame is independently decodable

        if (av_interleaved_write_frame(fmtCtx_, pkt_) < 0)
            std::cerr << "[avi] write error\n";

        meta_.record(/*nal_type placeholder -- not a meaningful concept for JPEG, unused by consumers*/ 1,
                     static_cast<uint32_t>(frameCount_),
                     std::chrono::system_clock::time_point(std::chrono::microseconds(timestampUs)),
                     frameSeq);
        ++frameCount_;
    }

    int close()
    {
        std::lock_guard<std::mutex> lk(mu_);
        av_write_trailer(fmtCtx_);
        avio_closep(&fmtCtx_->pb);
        meta_.close();
        return frameCount_;
    }

private:
    std::mutex       mu_;
    AVFormatContext* fmtCtx_     = nullptr;
    AVStream*        stream_     = nullptr;
    AVPacket*        pkt_        = nullptr;
    MetaWriter       meta_;
    int              frameCount_ = 0;
};

// ─────────────────────────────────────────────────────────────────────────────
// RawFrame / FrameQueue
//
// Decouples the UDP receive loop from JPEG capture (formerly H.264
// encoding, before that was moved off the real-time path entirely --
// see the file's top comment). A slow consumer stealing time from the
// receive loop was the original reason this queue exists: the thread
// didn't get back around to recv() in time, overflowing the kernel's
// UDP receive buffer and silently dropping packets -- surfacing later
// as "[raw] Dropped incomplete frame" once enough reassembly slots
// filled up with frames that could now never complete. The receive
// loop's only job per frame is now to push the raw YUV into this
// queue, which is fast enough to never be the bottleneck; a pool of
// capture worker threads (see captureWorkerLoop) drains it in parallel
// and does the actual (slower) JPEG compression at whatever pace it
// can collectively sustain.
// ─────────────────────────────────────────────────────────────────────────────
struct RawFrame {
    std::vector<uint8_t> yuv;
    int64_t              timestampUs;
    uint32_t             frameSeq;
};

class FrameQueue {
public:
    // maxSize bounds memory use if capture falls behind for a
    // sustained period (a genuine CPU-bound limit, not just a brief
    // stall) -- rather than growing unbounded or blocking the producer
    // (which would reintroduce the exact stall this queue exists to
    // avoid), push drops the OLDEST queued frame to make room, so
    // capture always works on the freshest data available.
    explicit FrameQueue(size_t maxSize) : maxSize_(maxSize) {}

    void push(RawFrame&& f)
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (frames_.size() >= maxSize_) {
            frames_.pop_front();
            ++dropped_;
            if (dropped_ == 1 || dropped_ % 30 == 0) {
                std::cerr << "[cap] Capture falling behind -- dropped "
                          << dropped_ << " queued frame(s) so far\n";
            }
        }
        frames_.push_back(std::move(f));
        cv_.notify_one();
    }

    // Blocks until a frame is available, shutdown is requested, or a
    // short timeout elapses (so the caller can re-check shutdown_
    // periodically without a dedicated wake channel for it).
    bool pop(RawFrame& out, const std::atomic<bool>& shutdown)
    {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait_for(lk, std::chrono::milliseconds(500),
                     [&] { return !frames_.empty() || shutdown.load(); });
        if (frames_.empty()) return false;
        out = std::move(frames_.front());
        frames_.pop_front();
        return true;
    }

    void wake() { cv_.notify_all(); } // lets a shutdown be noticed immediately

private:
    std::mutex              mu_;
    std::condition_variable cv_;
    std::deque<RawFrame>    frames_;
    size_t                  maxSize_;
    uint64_t                dropped_ = 0;
};

// ─────────────────────────────────────────────────────────────────────────────
// JpegReorderBuffer
//
// A single core's worth of MJPEG encoding can't keep up with 30fps at
// typical capture resolutions either (the same throughput problem that
// forced live H.264 encoding off the real-time path in the first
// place), so capture runs one independent JPEG encoder per online CPU
// core in parallel -- see captureWorkerLoop. That means frames finish
// out of frameSeq order: whichever worker happens to grab the shortest
// straw publishes first. Everything downstream (the capture file,
// which is read back sequentially and assumes strictly increasing
// timestamps for DTS pacing, and the rolling pre-buffer) needs frames
// delivered in strict frameSeq order, so this buffers early arrivals
// and releases them once the gap in front of them closes.
//
// frameSeq can also have genuine, permanent gaps: FrameQueue itself
// drops the oldest queued frame under sustained backpressure, so the
// "next" sequence number this is waiting for may simply never arrive.
// Rather than stall forever, once more than maxWindow frames are held
// pending, the lowest-numbered one is released anyway and the gap is
// skipped over.
// ─────────────────────────────────────────────────────────────────────────────
class JpegReorderBuffer {
public:
    using Ready = std::function<void(std::vector<uint8_t>, int64_t, uint32_t)>;

    JpegReorderBuffer(size_t maxWindow, Ready onReady)
        : maxWindow_(maxWindow), onReady_(std::move(onReady)) {}

    void submit(std::vector<uint8_t> jpeg, int64_t timestampUs, uint32_t frameSeq)
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!started_) { next_ = frameSeq; started_ = true; }

        // Wraparound-safe "frameSeq is older than what we already
        // released" check -- a straggler from a worker that finished
        // very late, after a skip-ahead already passed it by.
        if (static_cast<int32_t>(frameSeq - next_) < 0) return;

        pending_.emplace(frameSeq, Item{ std::move(jpeg), timestampUs });

        while (!pending_.empty()) {
            auto it = pending_.begin();
            bool isNext   = (it->first == next_);
            bool mustSkip = (pending_.size() > maxWindow_);
            if (!isNext && !mustSkip) break;

            uint32_t seq = it->first;
            Item     item = std::move(it->second);
            pending_.erase(it);
            next_ = seq + 1;
            onReady_(std::move(item.jpeg), item.timestampUs, seq);
        }
    }

private:
    struct Item {
        std::vector<uint8_t> jpeg;
        int64_t               timestampUs;
    };

    std::mutex               mu_;
    std::map<uint32_t, Item> pending_;
    size_t                   maxWindow_;
    Ready                    onReady_;
    uint32_t                 next_    = 0;
    bool                     started_ = false;
};

// ─────────────────────────────────────────────────────────────────────────────
// Recorder
// ─────────────────────────────────────────────────────────────────────────────
// Idle -> Recording -> Draining -> Idle. Closing a recording (writing
// the AVI trailer/index) is fast and synchronous -- there's no batch
// step to defer, so unlike an earlier version of this file there's no
// separate "Encoding" state between Draining and Idle.
enum class RecordState { Idle, Recording, Draining };

struct RecorderStatus {
    std::string state;   // "idle" | "recording" | "draining"
    std::string file;
};

class Recorder {
public:
    explicit Recorder(std::string rawHost,
                      int         rawPort,
                      int         rawWidth,
                      int         rawHeight,
                      int         rawFps,
                      int         rawStride,
                      std::string dir,
                      double preBufferSecs  = kDefaultPreSecs,
                      double postBufferSecs = kDefaultPostSecs)
        : rawHost_(std::move(rawHost))
        , rawPort_(rawPort)
        , rawWidth_(rawWidth)
        , rawHeight_(rawHeight)
        , rawFps_(rawFps)
        , rawStride_(rawStride > 0 ? rawStride : rawWidth)
        , dir_(std::move(dir))
        , preBuf_(preBufferSecs)
        , preBufferSecs_(preBufferSecs)
        , postBufferSecs_(postBufferSecs)
        // ~2s of headroom at the configured frame rate -- a safety net
        // for a brief capture stall, not a substitute for JPEG capture
        // actually keeping up on average (which it should, given the
        // worker pool below -- see FrameQueue's own comment).
        , frameQueue_(static_cast<size_t>(std::max(rawFps * 2, 30)))
        , numCaptureWorkers_(static_cast<int>(onlineCpuCount()))
        , reorder_(static_cast<size_t>(numCaptureWorkers_) * 2,
                   [this](std::vector<uint8_t> jpeg, int64_t ts, uint32_t seq) {
                       publishCapturedFrame(std::move(jpeg), ts, seq);
                   })
    {
        readerThread_ = std::thread([this]{ readStream(); });
        for (int i = 0; i < numCaptureWorkers_; ++i)
            captureThreads_.emplace_back([this, i]{ captureWorkerLoop(i); });
        drainThread_ = std::thread([this]{ drainLoop(); });
    }

    ~Recorder()
    {
        shutdown_ = true;
        drainCv_.notify_all();
        frameQueue_.wake();
        if (drainThread_.joinable())  drainThread_.join();
        if (readerThread_.joinable()) readerThread_.join();
        for (auto& t : captureThreads_) if (t.joinable()) t.join();
    }

    std::string start(const std::string& filename)
    {
        // ── already active: return current file ──────────────────────────────
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (state_ == RecordState::Recording) {
                std::cout << "[rec] Already recording: " << current_ << "\n";
                return current_;
            }
            if (state_ == RecordState::Draining) {
                std::cout << "[rec] Resuming — cancelling drain on: " << current_ << "\n";
                state_    = RecordState::Recording;
                drainCmd_ = DrainCmd::Resume;
                drainCv_.notify_all();
                return current_;
            }
            // Idle falls through to start a new recording.
        }

        fs::create_directories(dir_);
        std::string stem    = fs::path(filename).stem().string();
        std::string outPath = (fs::path(dir_) / (stem + ".avi")).string();

        if (fs::exists(outPath))
            throw std::runtime_error("file already exists: " + outPath);

        // A JPEG frame is already its own final stored format, so
        // there's no equivalent of the old live-H.264 design's "wait
        // up to 10s for SPS/PPS" step here -- capture just starts
        // writing whatever JPEG frames arrive next, straight into the
        // AVI file.
        auto writer = std::make_unique<AviWriter>(outPath, rawWidth_, rawHeight_, rawFps_);

        auto pre = preBuf_.drain();
        std::cout << "[rec] Pre-buffer: flushing " << pre.size() << " frames\n";
        for (auto& f : pre)
            writer->writeFrame(f.jpeg, f.timestampUs, f.frameSeq);

        {
            std::lock_guard<std::mutex> lk(mu_);
            aviWriter_ = std::move(writer);
            current_   = outPath;
            state_     = RecordState::Recording;
            drainCmd_  = DrainCmd::Wait;
        }

        std::cout << "[rec] Recording started: " << outPath << "\n";
        return outPath;
    }

    std::string stop()
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (state_ == RecordState::Idle)
            throw std::runtime_error("not recording");
        if (state_ == RecordState::Draining) {
            std::cout << "[rec] Already draining: " << current_ << "\n";
            return current_;
        }

        std::string saved = current_;
        state_    = RecordState::Draining;
        drainCmd_ = DrainCmd::Stop;
        stopTime_ = Clock::now();
        drainCv_.notify_all();
        std::cout << "[rec] " << toRFC3339(stopTime_)
                  << " stop requested — draining " << postBufferSecs_ << "s post-buffer\n";
        return saved;
    }

    RecorderStatus status()
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::string s = "idle";
        if (state_ == RecordState::Recording)     s = "recording";
        else if (state_ == RecordState::Draining) s = "draining";
        return { s, current_ };
    }

    double postBufferSecs() const { return postBufferSecs_; }
    const std::string& dir() const { return dir_; }

    // Waits for the first frame to actually be captured -- used only to
    // delay opening the TCP control port until picam-raw is confirmed
    // to be delivering frames (see Server::run). Unlike the old
    // SPS/PPS-based version of this, capture doesn't involve an H.264
    // encoder at all, so this just tracks "has anything been captured
    // yet".
    bool waitForStream(int timeoutSecs = 60)
    {
        std::unique_lock<std::mutex> lk(firstFrameMu_);
        return firstFrameCv_.wait_for(lk,
            std::chrono::seconds(timeoutSecs),
            [this]{ return firstFrameReady_.load(); });
    }

    bool streamReady() const { return firstFrameReady_.load(); }

private:
    // Dispatches one freshly JPEG-captured frame, already back in
    // strict frameSeq order courtesy of reorder_: always into the
    // rolling pre-buffer, and additionally straight into the current
    // recording's open AVI file if one is active.
    void onCapturedFrame(std::vector<uint8_t> jpeg, int64_t timestampUs, uint32_t frameSeq)
    {
        TP wallTime = std::chrono::system_clock::time_point(std::chrono::microseconds(timestampUs));

        std::lock_guard<std::mutex> lk(mu_);
        if ((state_ == RecordState::Recording || state_ == RecordState::Draining) && aviWriter_)
            aviWriter_->writeFrame(jpeg, timestampUs, frameSeq);
        preBuf_.push(std::move(jpeg), timestampUs, wallTime, frameSeq);
    }

    // reorder_'s onReady callback: called synchronously, one frame at a
    // time and always in ascending frameSeq order (see JpegReorderBuffer),
    // so this is also the correct, single place to detect "the very
    // first frame has now actually been published" -- unlike checking
    // this inside a capture worker directly, which could fire based on
    // whichever worker happened to finish its own encode first.
    void publishCapturedFrame(std::vector<uint8_t> jpeg, int64_t timestampUs, uint32_t frameSeq)
    {
        onCapturedFrame(std::move(jpeg), timestampUs, frameSeq);

        if (!firstFrameReady_.load()) {
            { std::lock_guard<std::mutex> lk(firstFrameMu_); firstFrameReady_ = true; }
            firstFrameCv_.notify_all();
        }
    }

    void drainLoop()
    {
        while (!shutdown_) {
            {
                std::unique_lock<std::mutex> lk(mu_);
                drainCv_.wait(lk, [this]{
                    return shutdown_.load() ||
                           drainCmd_ == DrainCmd::Stop ||
                           drainCmd_ == DrainCmd::Resume;
                });
                if (shutdown_) return;
                if (drainCmd_ == DrainCmd::Resume) {
                    drainCmd_ = DrainCmd::Wait;
                    continue;
                }
            }

            std::cout << "[rec] " << toRFC3339(Clock::now())
                      << " post-buffer: recording for " << postBufferSecs_ << "s more\n";

            bool resumed = false;
            {
                std::unique_lock<std::mutex> lk(mu_);
                resumed = drainCv_.wait_for(
                    lk,
                    std::chrono::duration<double>(postBufferSecs_),
                    [this]{ return shutdown_.load() || drainCmd_ == DrainCmd::Resume; });
            }

            if (shutdown_) return;

            if (resumed) {
                auto elapsed = std::chrono::duration<double>(Clock::now() - stopTime_).count();
                std::cout << "[rec] " << toRFC3339(Clock::now())
                          << " drain cancelled after " << std::fixed << std::setprecision(1)
                          << elapsed << "s — continuing recording\n";
                std::lock_guard<std::mutex> lk(mu_);
                drainCmd_ = DrainCmd::Wait;
                continue;
            }

            // Post-buffer fully drained -- finalize the AVI file
            // directly. A JPEG frame is already its own final stored
            // format, so unlike the old live-H.264 or JPEG-then-H.264
            // batch-transcode designs, there's no encode/mux work left
            // to defer: closing just means writing the AVI trailer/
            // index, fast enough to do right here.
            std::unique_ptr<AviWriter> writer;
            std::string outPath;
            {
                std::lock_guard<std::mutex> lk(mu_);
                writer    = std::move(aviWriter_);
                outPath   = current_;
                state_    = RecordState::Idle;
                current_  = "";
                drainCmd_ = DrainCmd::Wait;
            }

            int nFrames = writer->close();
            auto recSecs = std::chrono::duration<double>(Clock::now() - stopTime_).count();
            std::cout << "[rec] " << toRFC3339(Clock::now())
                      << " Recording finalized: " << outPath
                      << " (" << nFrames << " frames, "
                      << std::fixed << std::setprecision(1) << recSecs
                      << "s since stop)\n";
        }
    }

    // A JPEG encoder plus its persistent frame/packet, entirely owned by
    // one capture worker thread -- see captureWorkerLoop. JPEG frames
    // have no inter-frame dependencies, so N of these can run fully
    // independently in parallel, unlike the old single persistent H.264
    // encoder this replaced.
    struct JpegEncoderHandle {
        AVCodecContextPtr ctx;
        AVFramePtr        frame;
        AVPacketPtr       pkt;
    };

    // ── JPEG capture encoder init (one instance per capture worker) ──────────
    JpegEncoderHandle initJpegEncoder()
    {
        JpegEncoderHandle h;

        const AVCodec* codec = avcodec_find_encoder(AV_CODEC_ID_MJPEG);
        if (!codec)
            throw std::runtime_error("MJPEG encoder not found");

        h.ctx.reset(avcodec_alloc_context3(codec));
        if (!h.ctx)
            throw std::runtime_error("avcodec_alloc_context3 (mjpeg) failed");

        h.ctx->width          = rawWidth_;
        h.ctx->height         = rawHeight_;
        h.ctx->time_base      = { 1, rawFps_ };
        h.ctx->pix_fmt        = AV_PIX_FMT_YUVJ420P; // MJPEG's native full-range format
        // Quality: the mjpeg encoder honors qscale/global_quality via
        // FF_QP2LAMBDA scaling; 2-5 is visually near-lossless while
        // still compressing far enough (~10-20x vs raw) to keep the
        // per-worker capture file and pre-buffer memory use reasonable.
        h.ctx->flags         |= AV_CODEC_FLAG_QSCALE;
        h.ctx->global_quality = 4 * FF_QP2LAMBDA;

        int ret = avcodec_open2(h.ctx.get(), codec, nullptr);
        if (ret < 0) {
            char err[128]; av_strerror(ret, err, sizeof(err));
            throw std::runtime_error(std::string("avcodec_open2 (mjpeg): ") + err);
        }

        h.frame.reset(av_frame_alloc());
        if (!h.frame) throw std::runtime_error("av_frame_alloc (mjpeg) failed");
        h.frame->format = AV_PIX_FMT_YUVJ420P;
        h.frame->width  = rawWidth_;
        h.frame->height = rawHeight_;
        if (av_frame_get_buffer(h.frame.get(), 0) < 0)
            throw std::runtime_error("av_frame_get_buffer (mjpeg) failed");

        h.pkt.reset(av_packet_alloc());
        if (!h.pkt) throw std::runtime_error("av_packet_alloc (mjpeg) failed");

        return h;
    }

    // ── JPEG-encode one raw YUV420 frame and submit it for reordering ────────
    void captureFrame(JpegEncoderHandle& enc, int64_t& pts,
                       const std::vector<uint8_t>& yuv, int64_t timestampUs, uint32_t frameSeq)
    {
        const int    uvStride = rawStride_ / 2;
        const int    uvHeight = rawHeight_ / 2;
        const int    uvWidth  = rawWidth_  / 2;
        const size_t yBytes   = static_cast<size_t>(rawStride_) * rawHeight_;
        const size_t uvBytes  = static_cast<size_t>(uvStride) * uvHeight;

        if (yuv.size() < yBytes + 2 * uvBytes) {
            std::cerr << "[cap] Frame size " << yuv.size()
                      << " < expected " << (yBytes + 2 * uvBytes) << " — skipping\n";
            return;
        }

        if (av_frame_make_writable(enc.frame.get()) < 0) return;

        // Copy each plane row-by-row, stripping stride padding
        for (int row = 0; row < rawHeight_; ++row)
            std::memcpy(enc.frame->data[0] + row * enc.frame->linesize[0],
                        yuv.data() + row * rawStride_, rawWidth_);
        for (int row = 0; row < uvHeight; ++row)
            std::memcpy(enc.frame->data[1] + row * enc.frame->linesize[1],
                        yuv.data() + yBytes + row * uvStride, uvWidth);
        for (int row = 0; row < uvHeight; ++row)
            std::memcpy(enc.frame->data[2] + row * enc.frame->linesize[2],
                        yuv.data() + yBytes + uvBytes + row * uvStride, uvWidth);

        enc.frame->pts = pts++;

        if (avcodec_send_frame(enc.ctx.get(), enc.frame.get()) < 0) return;

        if (avcodec_receive_packet(enc.ctx.get(), enc.pkt.get()) == 0) {
            reorder_.submit(std::vector<uint8_t>(enc.pkt->data, enc.pkt->data + enc.pkt->size),
                             timestampUs, frameSeq);
            av_packet_unref(enc.pkt.get());
        }
    }

    // ── Connect to picam-raw UDP stream, reassemble frames, capture ───────────
    void connectRaw()
    {
        int sock = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (sock < 0)
            throw std::runtime_error("socket: " + std::string(strerror(errno)));

        // 1-second receive timeout so we can check shutdown_
        struct timeval tv{ 1, 0 };
        ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        // Large receive buffer to absorb UDP bursts
        int rcvBuf = 8 * 1024 * 1024;
        ::setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcvBuf, sizeof(rcvBuf));

        sockaddr_in serverAddr{};
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port   = htons(static_cast<uint16_t>(rawPort_));
        ::inet_pton(AF_INET, rawHost_.c_str(), &serverAddr.sin_addr);

        // Register with picam-raw (any datagram registers the sender as a client)
        const uint8_t regByte = 0x01;
        if (::sendto(sock, &regByte, 1, 0,
                     reinterpret_cast<const sockaddr*>(&serverAddr),
                     sizeof(serverAddr)) < 0) {
            ::close(sock);
            throw std::runtime_error("sendto (register): " + std::string(strerror(errno)));
        }
        std::cout << "[raw] Registered with " << rawHost_ << ":" << rawPort_ << "\n";

        struct PendingFrame {
            std::vector<std::vector<uint8_t>> chunks;
            uint16_t received    = 0;
            uint16_t total       = 0;
            int64_t  timestampUs = 0;
        };
        std::unordered_map<uint32_t, PendingFrame> pending;

        auto keepaliveAt = std::chrono::steady_clock::now();
        std::vector<uint8_t> buf(65536);

        while (!shutdown_) {
            // Keepalive: picam-raw removes clients silent for >10s
            {
                auto now = std::chrono::steady_clock::now();
                if (now - keepaliveAt >= std::chrono::seconds(5)) {
                    ::sendto(sock, &regByte, 1, 0,
                             reinterpret_cast<const sockaddr*>(&serverAddr),
                             sizeof(serverAddr));
                    keepaliveAt = now;
                }
            }

            ssize_t n = ::recv(sock, buf.data(), buf.size(), 0);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                ::close(sock);
                throw std::runtime_error("recv: " + std::string(strerror(errno)));
            }
            if (n < 8) continue;

            // Parse common 8-byte chunk header
            uint32_t frameSeq    = (uint32_t(buf[0])<<24)|(uint32_t(buf[1])<<16)|
                                   (uint32_t(buf[2])<< 8)| uint32_t(buf[3]);
            uint16_t chunkSeq    = (uint16_t(buf[4])<<8)| uint16_t(buf[5]);
            uint16_t totalChunks = (uint16_t(buf[6])<<8)| uint16_t(buf[7]);

            size_t  payloadOffset;
            int64_t timestampUs = 0;

            if (chunkSeq == 0) {
                // Chunk 0 has an extended 32-byte header with metadata
                if (n < 32) continue;
                for (int i = 0; i < 8; ++i)
                    timestampUs = (timestampUs << 8) | buf[8 + i];
                payloadOffset = 32;

                auto& pf       = pending[frameSeq];
                pf.total       = totalChunks;
                pf.timestampUs = timestampUs;
                pf.chunks.assign(totalChunks, {});
                pf.received    = 0;
            } else {
                payloadOffset = 8;
            }

            if (n <= static_cast<ssize_t>(payloadOffset)) continue;

            auto it = pending.find(frameSeq);
            if (it == pending.end()) continue;   // chunk 0 not yet seen for this frame
            PendingFrame& pf = it->second;
            if (chunkSeq >= pf.total) continue;
            if (pf.chunks[chunkSeq].empty()) {
                pf.chunks[chunkSeq].assign(buf.data() + payloadOffset, buf.data() + n);
                ++pf.received;
            }

            if (pf.received == pf.total) {
                // All chunks arrived — reassemble and hand off to the
                // capture thread via frameQueue_ rather than JPEG-
                // compressing inline here, so this loop can get back
                // around to recv() promptly regardless of how long
                // that takes, or incoming UDP chunks queue up in the
                // kernel and start getting dropped.
                std::vector<uint8_t> yuv;
                size_t totalSize = 0;
                for (auto& c : pf.chunks) totalSize += c.size();
                yuv.reserve(totalSize);
                for (auto& c : pf.chunks)
                    yuv.insert(yuv.end(), c.begin(), c.end());

                int64_t  ts  = pf.timestampUs;
                uint32_t seq = frameSeq;
                pending.erase(it);
                frameQueue_.push(RawFrame{ std::move(yuv), ts, seq });
            }

            // Prune excessively old incomplete frames to cap memory use
            if (pending.size() > 120) {
                auto oldest = pending.begin();
                std::cerr << "[raw] Dropped incomplete frame " << oldest->first << "\n";
                pending.erase(oldest);
            }
        }

        ::close(sock);
    }

    void readStream()
    {
        while (!shutdown_) {
            try { connectRaw(); }
            catch (const std::exception& e) {
                std::cerr << "[raw] Error: " << e.what() << " — retrying in 3s\n";
            }
            if (!shutdown_)
                std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }

    // One of numCaptureWorkers_ parallel consumers of frameQueue_, each
    // with its own independent JPEG encoder -- independent of the
    // receive loop's own pace, see FrameQueue's comment for why capture
    // is split off from it onto separate threads in the first place.
    // A single core's worth of MJPEG encoding turned out not to be
    // enough to sustain 30fps either (the exact same throughput problem
    // that forced live H.264 off the real-time path to begin with), so
    // -- unlike the very first version of this pipeline -- capture now
    // widens its own affinity too and runs one encoder per online core.
    // Frames finish out of order across workers; reorder_ puts them
    // back in sequence before anything downstream sees them.
    void captureWorkerLoop(int workerIdx)
    {
        widenAffinityToAllCores("[cap]");

        JpegEncoderHandle enc;
        try {
            enc = initJpegEncoder();
        } catch (const std::exception& e) {
            std::cerr << "[cap] Worker " << workerIdx << " encoder init failed: " << e.what() << "\n";
            return;
        }
        std::cout << "[cap] Worker " << workerIdx << " ready: " << rawWidth_ << "x" << rawHeight_
                  << " @" << rawFps_ << "fps  stride=" << rawStride_ << "\n";

        int64_t  pts = 0;
        RawFrame f;
        while (!shutdown_) {
            if (!frameQueue_.pop(f, shutdown_)) continue; // timed out re-checking shutdown_, or woken by it
            captureFrame(enc, pts, f.yuv, f.timestampUs, f.frameSeq);
        }
    }

    // Widens this thread's own CPU affinity to every online core EXCEPT
    // kReservedCore. Every thread inherits main()'s core-2-only
    // affinity mask (set before any thread here existed) unless it
    // explicitly changes its own -- that's fine, intentional even, for
    // the receive/TCP-control threads, which stay lightweight and need
    // that core kept genuinely uncontended (see kReservedCore's own
    // comment: sharing it with a CPU-bound thread starves the receive
    // loop and reintroduces "[raw] Dropped incomplete frame" one layer
    // up, as happened when this first widened to *every* core including
    // the reserved one). The capture workers and the transcode thread
    // both need more than one core: libx264 (transcode) and the mjpeg
    // encoder (capture) both auto-detect their own internal thread
    // count from how many CPUs are visible to the calling thread's
    // affinity mask (av_cpu_count() reads sched_getaffinity()), and
    // running N independent worker threads (capture) each eligible for
    // every non-reserved core lets the kernel schedule them in parallel
    // instead of stacking them onto one core.
    static void widenAffinityToAllCores(const char* logTag)
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        long nCpus = onlineCpuCount();
        for (long i = 0; i < nCpus; ++i)
            if (static_cast<int>(i) != kReservedCore) CPU_SET(static_cast<int>(i), &cpuset);
        if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0)
            std::cerr << logTag << " Warning: failed to widen thread's CPU affinity: " << strerror(errno) << "\n";
        else
            std::cout << logTag << " Thread using up to " << CPU_COUNT(&cpuset)
                      << " CPU(s) (core " << kReservedCore << " reserved for receive/control)\n";
    }

    // ── members ──────────────────────────────────────────────────────────────
    std::mutex                     mu_;
    RecordState                    state_   = RecordState::Idle;
    std::string                    current_;
    std::unique_ptr<AviWriter>     aviWriter_;
    std::string                    rawHost_;
    int                            rawPort_;
    int                            rawWidth_;
    int                            rawHeight_;
    int                            rawFps_;
    int                            rawStride_;
    std::string                    dir_;
    RollingBuffer                  preBuf_;
    double                         preBufferSecs_;
    double                         postBufferSecs_;
    FrameQueue                     frameQueue_; // reassembled raw frames awaiting JPEG capture -- see its own comment

    // One JPEG capture worker per online CPU core, plus the buffer that
    // re-serializes their out-of-order output -- see captureWorkerLoop
    // and JpegReorderBuffer's own comments.
    int                             numCaptureWorkers_;
    JpegReorderBuffer               reorder_;

    std::thread                    readerThread_;
    std::vector<std::thread>       captureThreads_;
    std::thread                    drainThread_;
    std::atomic<bool>              shutdown_{ false };

    std::condition_variable        drainCv_;
    TP                             stopTime_;

    enum class DrainCmd { Wait, Stop, Resume };
    DrainCmd                       drainCmd_ = DrainCmd::Wait;

    // Tracks "has anything been captured yet" -- see waitForStream.
    std::mutex                     firstFrameMu_;
    std::atomic<bool>              firstFrameReady_{ false };
    std::condition_variable        firstFrameCv_;
};

// ─────────────────────────────────────────────────────────────────────────────
// ControlServer — TCP, plain-text line protocol
// ─────────────────────────────────────────────────────────────────────────────
class Server {
public:
    Server(Recorder& rec, int port) : rec_(rec), port_(port) {}
    ~Server() { if (listenFd_ >= 0) ::close(listenFd_); }

    void run()
    {
        listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listenFd_ < 0)
            throw std::runtime_error(std::string("socket: ") + strerror(errno));

        int opt = 1;
        ::setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(static_cast<uint16_t>(port_));

        if (::bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
            throw std::runtime_error(std::string("bind: ") + strerror(errno));
        if (::listen(listenFd_, 8) < 0)
            throw std::runtime_error(std::string("listen: ") + strerror(errno));

        // Wait for the first encoded frame to deliver SPS/PPS before accepting commands
        std::cout << "[tcp] Waiting for stream before accepting connections...\n";
        if (!rec_.waitForStream(60)) {
            std::cerr << "[tcp] Timed out waiting for stream — "
                         "still opening control port but stream may not be ready\n";
        } else {
            std::cout << "[tcp] Stream ready — listening on 0.0.0.0:" << port_ << "\n";
        }

        while (true) {
            int clientFd = ::accept(listenFd_, nullptr, nullptr);
            if (clientFd < 0) {
                if (errno == EINTR) continue;
                std::cerr << "[tcp] accept: " << strerror(errno) << "\n";
                break;
            }
            std::thread([this, clientFd]{ handleClient(clientFd); }).detach();
        }
    }

private:
    void handleClient(int fd)
    {
        FILE* fp = ::fdopen(fd, "r+");
        if (!fp) { ::close(fd); return; }

        char line[4096];
        while (std::fgets(line, sizeof(line), fp)) {
            size_t len = std::strlen(line);
            while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r'))
                line[--len] = '\0';
            if (len == 0) continue;

            std::string reply = dispatch(line);
            if (std::fwrite(reply.data(), 1, reply.size(), fp) != reply.size())
                break;
            std::fflush(fp);
        }
        std::fclose(fp);
    }

    static std::string ok(std::initializer_list<std::pair<std::string,std::string>> kv)
    {
        std::string out = "ok=true\n";
        for (auto& [k, v] : kv) out += k + "=" + v + "\n";
        out += "\n";
        return out;
    }

    static std::string err(const std::string& msg)
    {
        return "ok=false\nerror=" + msg + "\n\n";
    }

    std::string dispatch(const char* raw)
    {
        std::string line(raw);
        auto sp  = line.find(' ');
        std::string cmd = (sp == std::string::npos) ? line : line.substr(0, sp);
        std::string arg = (sp == std::string::npos) ? "" : line.substr(sp + 1);
        while (!arg.empty() && (arg.back() == ' ' || arg.back() == '\t')) arg.pop_back();

        if (cmd == "start") {
            if (arg.empty()) return err("usage: start <name>");
            try {
                std::string path = rec_.start(arg);
                return ok({{"file", path}});
            } catch (const std::exception& e) { return err(e.what()); }
        }

        if (cmd == "stop") {
            try {
                std::string path = rec_.stop();
                return ok({{"file", path},
                           {"note", "draining " +
                                    std::to_string((int)rec_.postBufferSecs()) +
                                    "s post-buffer"}});
            } catch (const std::exception& e) { return err(e.what()); }
        }

        if (cmd == "status") {
            auto st = rec_.status();
            return ok({{"state", st.state}, {"file", st.file}});
        }

        if (cmd == "list") {
            std::string out = "ok=true\n";
            std::error_code ec;
            for (auto& entry : fs::directory_iterator(rec_.dir(), ec)) {
                if (entry.is_directory()) continue;
                auto sz = entry.file_size(ec);
                struct stat st{};
                TP modTime = Clock::now();
                if (::stat(entry.path().c_str(), &st) == 0)
                    modTime = Clock::from_time_t(st.st_mtime);
                out += "file=" + entry.path().filename().string() +
                       " size=" + std::to_string(ec ? 0 : (int64_t)sz) +
                       " modified=" + toRFC3339(modTime) + "\n";
            }
            out += "\n";
            return out;
        }

        return err("unknown command: " + cmd);
    }

    Recorder& rec_;
    int       port_;
    int       listenFd_ = -1;
};

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[])
{
    std::cout.setf(std::ios::unitbuf);
    std::cerr.setf(std::ios::unitbuf);

    // ── 0. Pin this process to the reserved CPU core ──────────────────────────
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(kReservedCore, &cpuset);
        if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0)
            std::cerr << "[main] Warning: failed to set CPU affinity: " << strerror(errno) << "\n";
        else
            std::cout << "[main] Pinned to CPU core " << kReservedCore << "\n";
    }

    // ── 1. Determine config file path ─────────────────────────────────────────
    std::string configPath;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--config" && i + 1 < argc) { configPath = argv[++i]; break; }
    }
    if (configPath.empty() && fs::exists("recorder.ini"))
        configPath = "recorder.ini";

    // ── 2. Load config file (if any) ─────────────────────────────────────────
    Config cfg;
    applySharedStreamDefaults(cfg);
    if (!configPath.empty()) {
        try {
            cfg = loadIni(configPath);
            std::cout << "[cfg] Loaded: " << configPath << "\n";
        } catch (const std::exception& e) {
            std::cerr << "[cfg] Error: " << e.what() << "\n";
            return 1;
        }
    }

    // ── 3. CLI flags override config values ──────────────────────────────────
    for (int i = 1; i < argc; ++i) {
        if      (std::string(argv[i]) == "--raw-host"   && i+1 < argc) cfg.rawHost   = argv[++i];
        else if (std::string(argv[i]) == "--raw-port"   && i+1 < argc) cfg.rawPort   = std::stoi(argv[++i]);
        else if (std::string(argv[i]) == "--raw-width"  && i+1 < argc) cfg.rawWidth  = std::stoi(argv[++i]);
        else if (std::string(argv[i]) == "--raw-height" && i+1 < argc) cfg.rawHeight = std::stoi(argv[++i]);
        else if (std::string(argv[i]) == "--raw-fps"    && i+1 < argc) cfg.rawFps    = std::stoi(argv[++i]);
        else if (std::string(argv[i]) == "--raw-stride" && i+1 < argc) cfg.rawStride = std::stoi(argv[++i]);
        else if (std::string(argv[i]) == "--dir"        && i+1 < argc) cfg.dir       = argv[++i];
        else if (std::string(argv[i]) == "--port"       && i+1 < argc) cfg.port      = std::stoi(argv[++i]);
        else if (std::string(argv[i]) == "--pre"        && i+1 < argc) cfg.preSecs   = std::stod(argv[++i]);
        else if (std::string(argv[i]) == "--post"       && i+1 < argc) cfg.postSecs  = std::stod(argv[++i]);
        else if (std::string(argv[i]) == "--config"     && i+1 < argc) ++i;
    }

    // ── 4. Start ──────────────────────────────────────────────────────────────
    av_log_set_callback([](void* avcl, int level, const char* fmt, va_list vl) {
        if (level > AV_LOG_WARNING) return;
        const char* noisy[] = {
            "non-existing PPS", "decode_slice_header error",
            "no frame!", "non-existing SPS", nullptr
        };
        for (const char** p = noisy; *p; ++p)
            if (std::strstr(fmt, *p)) return;
        av_log_default_callback(avcl, level, fmt, vl);
    });

    int effectiveStride = cfg.rawStride > 0 ? cfg.rawStride : cfg.rawWidth;
    std::cout << "[main] Raw source:   " << cfg.rawHost << ":" << cfg.rawPort << "\n"
              << "[main] Frame size:   " << cfg.rawWidth << "x" << cfg.rawHeight
              << "  stride=" << effectiveStride << "  fps=" << cfg.rawFps << "\n"
              << "[main] Recordings:   " << cfg.dir     << "\n"
              << "[main] Pre-buffer:   " << cfg.preSecs << "s\n"
              << "[main] Post-buffer:  " << cfg.postSecs << "s\n"
              << "[main] Control TCP:  0.0.0.0:" << cfg.port << "\n"
              << "[main]   echo 'start 111-222-333' | nc 127.0.0.1 " << cfg.port << "\n"
              << "[main]   echo 'stop'              | nc 127.0.0.1 " << cfg.port << "\n"
              << "[main]   echo 'status'            | nc 127.0.0.1 " << cfg.port << "\n"
              << "[main]   echo 'list'              | nc 127.0.0.1 " << cfg.port << "\n";

    Recorder rec(cfg.rawHost, cfg.rawPort,
                 cfg.rawWidth, cfg.rawHeight, cfg.rawFps, cfg.rawStride,
                 cfg.dir, cfg.preSecs, cfg.postSecs);
    Server   srv(rec, cfg.port);
    srv.run();

    return 0;
}
