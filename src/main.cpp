/**
 * picam_recorder — records raw YUV420 frames from picam-raw UDP stream to MP4
 *
 * Two-phase pipeline:
 *   1. Capture (real-time, must never fall behind): each raw YUV420
 *      frame is JPEG-compressed -- fast, no inter-frame dependencies --
 *      and either kept in a rolling pre-buffer (no recording active)
 *      or appended to the current recording's on-disk capture file.
 *      Live H.264 encoding used to happen here instead; it couldn't
 *      reliably sustain 30fps at typical recording resolutions even
 *      after being moved off the UDP receive thread and given more CPU
 *      cores, so it was removed from this real-time path entirely.
 *   2. Transcode (batch, once a recording finishes): the capture file
 *      is read back, JPEG-decoded, and encoded to H.264 with FFmpeg
 *      (libx264), then muxed into the final MP4 -- with no real-time
 *      deadline this time, since all the source data already exists on
 *      disk by the time this runs.
 *
 * Dependencies:
 *   - FFmpeg libs: libavformat, libavcodec, libavutil, libswscale
 *   No other external dependencies.
 *
 * Build example (Linux / macOS):
 *   g++ -std=c++17 -O2 -pthread \
 *       main.cpp \
 *       $(pkg-config --cflags --libs libavformat libavcodec libavutil libswscale) \
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
 *     start <filename.mp4>
 *     stop
 *     status
 *     list
 *
 *   Each response is one or more  key=value  lines followed by a blank line.
 *
 *   Quick test with nc:
 *     echo 'start clip01.mp4' | nc 127.0.0.1 8080
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
#include <libswscale/swscale.h>
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
#include <iomanip>
#include <iostream>
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
// RAII wrappers for FFmpeg's C-API alloc/free pairs -- used for both the
// persistent JPEG capture encoder and the per-job transcode encoder/
// decoder/scaler, so none of that state needs manual cleanup at every
// throw/return point.
// ─────────────────────────────────────────────────────────────────────────────
struct AVCodecContextDeleter { void operator()(AVCodecContext* p) const { if (p) avcodec_free_context(&p); } };
struct AVFrameDeleter        { void operator()(AVFrame* p)        const { if (p) av_frame_free(&p); } };
struct AVPacketDeleter       { void operator()(AVPacket* p)       const { if (p) av_packet_free(&p); } };
struct SwsContextDeleter     { void operator()(SwsContext* p)     const { if (p) sws_freeContext(p); } };
using AVCodecContextPtr = std::unique_ptr<AVCodecContext, AVCodecContextDeleter>;
using AVFramePtr        = std::unique_ptr<AVFrame, AVFrameDeleter>;
using AVPacketPtr       = std::unique_ptr<AVPacket, AVPacketDeleter>;
using SwsContextPtr     = std::unique_ptr<SwsContext, SwsContextDeleter>;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────
static constexpr int    kDefaultPort     = 8080;
static constexpr double kDefaultPreSecs  = 10.0;
static constexpr double kDefaultPostSecs = 10.0;

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
// H.264 SPS dimension parser
//
// Reads pic_width_in_mbs_minus1 and pic_height_in_map_units_minus1 from a
// raw SPS NAL (without the leading start code or NAL header byte).
// Implements just enough of ISO 14496-10 sec 7.3.2.1.1 to reach those fields.
// ─────────────────────────────────────────────────────────────────────────────
struct BitReader {
    const uint8_t* data;
    size_t         size;   // bytes
    size_t         pos;    // current bit position

    explicit BitReader(const std::vector<uint8_t>& v)
        : data(v.data()), size(v.size()), pos(0) {}

    uint32_t readBit()
    {
        if (pos >= size * 8) return 0;
        uint32_t b = (data[pos / 8] >> (7 - pos % 8)) & 1;
        ++pos;
        return b;
    }

    uint32_t readBits(int n)
    {
        uint32_t v = 0;
        for (int i = 0; i < n; ++i) v = (v << 1) | readBit();
        return v;
    }

    // Exp-Golomb unsigned
    uint32_t readUE()
    {
        int zeros = 0;
        while (readBit() == 0 && pos < size * 8) ++zeros;
        if (zeros == 0) return 0;
        return (1u << zeros) - 1 + readBits(zeros);
    }

    // Exp-Golomb signed
    int32_t readSE()
    {
        uint32_t v = readUE();
        return (v & 1) ? (int32_t)((v + 1) / 2) : -(int32_t)(v / 2);
    }
};

// Returns {width, height} or {0,0} on failure.
// Expects the SPS payload WITHOUT the leading NAL header byte (0x67).
static std::pair<int,int> parseSPSDimensions(const std::vector<uint8_t>& sps)
{
    if (sps.size() < 3) return {0, 0};

    uint8_t profile_idc = sps[0];
    BitReader br(sps);

    br.readBits(8);  // profile_idc
    br.readBits(8);  // constraint flags
    br.readBits(8);  // level_idc

    br.readUE();     // seq_parameter_set_id

    if (profile_idc == 100 || profile_idc == 110 || profile_idc == 122 ||
        profile_idc == 244 || profile_idc ==  44 || profile_idc ==  83 ||
        profile_idc ==  86 || profile_idc == 118 || profile_idc == 128 ||
        profile_idc == 138 || profile_idc == 139 || profile_idc == 134) {
        uint32_t chroma_format_idc = br.readUE();
        if (chroma_format_idc == 3) br.readBit(); // separate_colour_plane_flag
        br.readUE();   // bit_depth_luma_minus8
        br.readUE();   // bit_depth_chroma_minus8
        br.readBit();  // qpprime_y_zero_transform_bypass_flag
        if (br.readBit()) { // seq_scaling_matrix_present_flag
            int n = (chroma_format_idc != 3) ? 8 : 12;
            for (int i = 0; i < n; ++i) {
                if (br.readBit()) { // seq_scaling_list_present_flag[i]
                    int sz = (i < 6) ? 16 : 64;
                    int lastScale = 8, nextScale = 8;
                    for (int j = 0; j < sz; ++j) {
                        if (nextScale != 0) {
                            int delta = br.readSE();
                            nextScale = (lastScale + delta + 256) % 256;
                        }
                        lastScale = (nextScale == 0) ? lastScale : nextScale;
                    }
                }
            }
        }
    }

    br.readUE();  // log2_max_frame_num_minus4
    uint32_t pic_order_cnt_type = br.readUE();
    if (pic_order_cnt_type == 0) {
        br.readUE(); // log2_max_pic_order_cnt_lsb_minus4
    } else if (pic_order_cnt_type == 1) {
        br.readBit(); // delta_pic_order_always_zero_flag
        br.readSE();  // offset_for_non_ref_pic
        br.readSE();  // offset_for_top_to_bottom_field
        uint32_t n = br.readUE(); // num_ref_frames_in_pic_order_cnt_cycle
        for (uint32_t i = 0; i < n; ++i) br.readSE();
    }

    br.readUE();   // max_num_ref_frames
    br.readBit();  // gaps_in_frame_num_value_allowed_flag

    uint32_t pic_width_in_mbs        = br.readUE() + 1;
    uint32_t pic_height_in_map_units = br.readUE() + 1;
    uint32_t frame_mbs_only_flag     = br.readBit();

    int width  = static_cast<int>(pic_width_in_mbs        * 16);
    int height = static_cast<int>(pic_height_in_map_units * 16 * (frame_mbs_only_flag ? 1 : 2));

    // Crop if cropping rectangle is present
    if (!frame_mbs_only_flag) br.readBit(); // mb_adaptive_frame_field_flag
    br.readBit(); // direct_8x8_inference_flag
    if (br.readBit()) { // frame_cropping_flag
        uint32_t crop_l = br.readUE();
        uint32_t crop_r = br.readUE();
        uint32_t crop_t = br.readUE();
        uint32_t crop_b = br.readUE();
        width  -= static_cast<int>((crop_l + crop_r) * 2);
        height -= static_cast<int>((crop_t + crop_b) * 2 * (frame_mbs_only_flag ? 1 : 2));
    }

    if (width <= 0 || height <= 0) return {0, 0};
    return {width, height};
}


class MP4Muxer {
public:
    MP4Muxer(const std::string& path,
             const std::vector<uint8_t>& sps,
             const std::vector<uint8_t>& pps)
        : meta_(path)
    {
        if (avformat_alloc_output_context2(&fmtCtx_, nullptr, "mp4",
                                           path.c_str()) < 0)
            throw std::runtime_error("avformat_alloc_output_context2 failed");

        stream_ = avformat_new_stream(fmtCtx_, nullptr);
        if (!stream_) throw std::runtime_error("avformat_new_stream failed");

        stream_->codecpar->codec_type = AVMEDIA_TYPE_VIDEO;
        stream_->codecpar->codec_id   = AV_CODEC_ID_H264;
        stream_->codecpar->format     = AV_PIX_FMT_YUV420P;
        stream_->codecpar->bit_rate   = 0;
        stream_->time_base            = { 1, 90000 };

        // SPS and PPS as stored include the NAL header byte (0x67 / 0x68).
        // The AVCC extradata and dimension parser both need the payload without it.
        const std::vector<uint8_t> spsPayload(sps.size() > 1 ? sps.begin() + 1 : sps.end(), sps.end());
        const std::vector<uint8_t> ppsPayload(pps.size() > 1 ? pps.begin() + 1 : pps.end(), pps.end());

        // ── Parse width/height directly from the SPS NAL bitstream ───────────
        {
            auto [w, h] = parseSPSDimensions(spsPayload);
            if (w > 0 && h > 0) {
                stream_->codecpar->width  = w;
                stream_->codecpar->height = h;
                std::cout << "[mp4] Dimensions: " << w << "x" << h << "\n";
            } else {
                std::ostringstream hex;
                for (auto b : sps)
                    hex << std::hex << std::setw(2) << std::setfill('0') << (int)b << ' ';
                std::cerr << "[mp4] Warning: could not parse SPS dimensions. "
                          << "SPS (" << sps.size() << " bytes): " << hex.str() << "\n";
            }
        }

        // ── Build AVCC extradata (SPS + PPS payloads, no NAL header bytes) ───
        // Format: [0x01][profile][compat][level][0xff][0xe1]
        //         [sps_len_hi][sps_len_lo][sps_payload...]
        //         [0x01][pps_len_hi][pps_len_lo][pps_payload...]
        {
            size_t extSize = 6 + 2 + spsPayload.size() + 1 + 2 + ppsPayload.size();
            uint8_t* ext = static_cast<uint8_t*>(av_malloc(extSize + AV_INPUT_BUFFER_PADDING_SIZE));
            if (ext) {
                size_t i = 0;
                ext[i++] = 0x01;                                          // configurationVersion
                ext[i++] = spsPayload.size() > 0 ? spsPayload[0] : 0x42; // AVCProfileIndication
                ext[i++] = spsPayload.size() > 1 ? spsPayload[1] : 0x00; // profile_compatibility
                ext[i++] = spsPayload.size() > 2 ? spsPayload[2] : 0x1e; // AVCLevelIndication
                ext[i++] = 0xff;                                          // lengthSizeMinusOne = 3
                ext[i++] = 0xe1;                                          // numSequenceParameterSets = 1
                ext[i++] = (spsPayload.size() >> 8) & 0xff;
                ext[i++] =  spsPayload.size()       & 0xff;
                std::memcpy(ext + i, spsPayload.data(), spsPayload.size()); i += spsPayload.size();
                ext[i++] = 0x01;                                          // numPictureParameterSets = 1
                ext[i++] = (ppsPayload.size() >> 8) & 0xff;
                ext[i++] =  ppsPayload.size()       & 0xff;
                std::memcpy(ext + i, ppsPayload.data(), ppsPayload.size());
                std::memset(ext + extSize, 0, AV_INPUT_BUFFER_PADDING_SIZE);
                stream_->codecpar->extradata      = ext;
                stream_->codecpar->extradata_size = static_cast<int>(extSize);
            }
        }

        if (avio_open(&fmtCtx_->pb, path.c_str(), AVIO_FLAG_WRITE) < 0)
            throw std::runtime_error("avio_open failed for: " + path);

        if (avformat_write_header(fmtCtx_, nullptr) < 0)
            throw std::runtime_error("avformat_write_header failed");

        pkt_ = av_packet_alloc();
        if (!pkt_) throw std::runtime_error("av_packet_alloc failed");
    }

    ~MP4Muxer()
    {
        if (pkt_)    av_packet_free(&pkt_);
        if (fmtCtx_) avformat_free_context(fmtCtx_);
    }

    MP4Muxer(const MP4Muxer&)            = delete;
    MP4Muxer& operator=(const MP4Muxer&) = delete;

    void writeNALU(const std::vector<uint8_t>& nalu, uint32_t dts, TP wallTime, uint32_t frameSeq)
    {
        std::lock_guard<std::mutex> lk(mu_);

        // Accumulate a monotonic 64-bit DTS from the 32-bit RTP-scale timestamp.
        // Use signed 32-bit delta so wraparound is handled correctly.
        if (!baseSet_) {
            prevRTP_    = dts;
            accumDTS_   = 0;
            lastDelta_  = 3000; // safe initial fallback (~30fps at 90kHz)
            baseSet_    = true;
        } else {
            int32_t delta = static_cast<int32_t>(dts - prevRTP_);
            if (delta > 0) {
                lastDelta_ = static_cast<uint32_t>(delta);
                accumDTS_ += lastDelta_;
            } else {
                accumDTS_ += lastDelta_;
            }
            prevRTP_ = dts;
        }

        uint64_t normDTS = accumDTS_;

        // The extradata built in the constructor declares AVCC format
        // with lengthSizeMinusOne = 3 (4-byte length-prefixed samples,
        // per ISO/IEC 14496-15) -- so each sample written here must be
        // prefixed with its own big-endian 4-byte length, NOT an
        // Annex-B start code. Writing a start code instead (as this
        // used to) leaves the moov/extradata correctly declaring AVCC
        // while every sample's payload is actually Annex-B: any
        // strict demuxer reads the first 4 bytes of a sample as a
        // length, gets "1" from the 00 00 00 01 start code, and
        // desyncs from there -- the container is well-formed but the
        // bitstream inside it is garbage, so the file fails to play
        // almost everywhere except a few very lenient players.
        std::vector<uint8_t> avcc;
        avcc.reserve(4 + nalu.size());
        const uint32_t naluLen = static_cast<uint32_t>(nalu.size());
        avcc.push_back(static_cast<uint8_t>((naluLen >> 24) & 0xff));
        avcc.push_back(static_cast<uint8_t>((naluLen >> 16) & 0xff));
        avcc.push_back(static_cast<uint8_t>((naluLen >> 8)  & 0xff));
        avcc.push_back(static_cast<uint8_t>( naluLen        & 0xff));
        avcc.insert(avcc.end(), nalu.begin(), nalu.end());

        av_packet_unref(pkt_);
        pkt_->data         = avcc.data();
        pkt_->size         = static_cast<int>(avcc.size());
        pkt_->stream_index = stream_->index;
        pkt_->pts          = static_cast<int64_t>(normDTS);
        pkt_->dts          = static_cast<int64_t>(normDTS);
        pkt_->duration     = static_cast<int64_t>(lastDelta_);
        if ((nalu[0] & 0x1F) == 5) pkt_->flags |= AV_PKT_FLAG_KEY;

        if (av_interleaved_write_frame(fmtCtx_, pkt_) < 0)
            std::cerr << "[mp4] write error\n";

        meta_.record(static_cast<int>(nalu[0] & 0x1F), dts, wallTime, frameSeq);
        ++naluCount_;
    }

    int close()
    {
        std::lock_guard<std::mutex> lk(mu_);
        av_write_trailer(fmtCtx_);
        avio_closep(&fmtCtx_->pb);
        meta_.close();
        return naluCount_;
    }

private:
    std::mutex       mu_;
    AVFormatContext* fmtCtx_    = nullptr;
    AVStream*        stream_    = nullptr;
    AVPacket*        pkt_       = nullptr;
    MetaWriter       meta_;
    uint32_t         prevRTP_   = 0;
    uint64_t         accumDTS_  = 0;
    uint32_t         lastDelta_ = 3000;
    bool             baseSet_   = false;
    int              naluCount_ = 0;
};

// ─────────────────────────────────────────────────────────────────────────────
// CaptureWriter / CaptureReader
//
// The on-disk intermediate for an in-progress recording: a flat,
// sequential dump of JPEG-compressed frames (see the file's top
// comment for why JPEG rather than literal raw YUV, and why capture is
// split from H.264 encoding at all). Format is deliberately as simple
// as possible -- write-once, read-once-sequentially, no container or
// indexing needed since the transcode worker is the only reader and
// always consumes it start-to-end exactly once, streaming (not loading
// the whole file into memory, which could be sizeable for a long
// recording even at JPEG's much smaller footprint than raw):
//
//   repeated: [frameSeq:4][timestampUs:8][jpegLen:4][jpeg bytes...]
//
// all integers big-endian.
// ─────────────────────────────────────────────────────────────────────────────
class CaptureWriter {
public:
    explicit CaptureWriter(std::string path)
        : path_(std::move(path))
        , f_(path_, std::ios::binary | std::ios::trunc)
    {
        if (!f_) throw std::runtime_error("cannot open capture file: " + path_);
    }

    const std::string& path() const { return path_; }

    void appendFrame(const std::vector<uint8_t>& jpeg, int64_t timestampUs, uint32_t frameSeq)
    {
        std::lock_guard<std::mutex> lk(mu_);
        writeU32(frameSeq);
        writeU64(static_cast<uint64_t>(timestampUs));
        writeU32(static_cast<uint32_t>(jpeg.size()));
        f_.write(reinterpret_cast<const char*>(jpeg.data()), static_cast<std::streamsize>(jpeg.size()));
        ++frameCount_;
    }

    int close()
    {
        std::lock_guard<std::mutex> lk(mu_);
        f_.flush();
        f_.close();
        return frameCount_;
    }

private:
    void writeU32(uint32_t v)
    {
        char b[4] = { char(v >> 24), char(v >> 16), char(v >> 8), char(v) };
        f_.write(b, 4);
    }
    void writeU64(uint64_t v)
    {
        char b[8];
        for (int i = 0; i < 8; ++i) b[i] = char(v >> (56 - 8 * i));
        f_.write(b, 8);
    }

    std::mutex    mu_;
    std::string   path_;
    std::ofstream f_;
    int           frameCount_ = 0;
};

struct CapturedFrame {
    std::vector<uint8_t> jpeg;
    int64_t               timestampUs;
    uint32_t               frameSeq;
};

// Sequential, one-frame-at-a-time reader for what CaptureWriter wrote
// -- deliberately not a batch "read it all into a vector" API, so a
// long recording's transcode doesn't need to hold every captured frame
// in memory at once.
class CaptureReader {
public:
    explicit CaptureReader(const std::string& path) : f_(path, std::ios::binary) {}

    bool ok() const { return static_cast<bool>(f_); }

    // Returns false at end-of-file or on any truncated/malformed
    // trailing record (treated the same as a clean EOF -- a recording
    // whose capture file got cut short still transcodes whatever
    // complete frames it has rather than failing outright).
    bool readNext(CapturedFrame& out)
    {
        uint32_t frameSeq = 0;
        uint64_t tsU64    = 0;
        uint32_t jpegLen  = 0;
        if (!readU32(frameSeq)) return false;
        if (!readU64(tsU64))    return false;
        if (!readU32(jpegLen))  return false;

        out.jpeg.resize(jpegLen);
        if (jpegLen > 0) {
            f_.read(reinterpret_cast<char*>(out.jpeg.data()), jpegLen);
            if (f_.gcount() != static_cast<std::streamsize>(jpegLen)) return false;
        }
        out.timestampUs = static_cast<int64_t>(tsU64);
        out.frameSeq    = frameSeq;
        return true;
    }

private:
    bool readU32(uint32_t& v)
    {
        unsigned char b[4];
        f_.read(reinterpret_cast<char*>(b), 4);
        if (f_.gcount() != 4) return false;
        v = (uint32_t(b[0]) << 24) | (uint32_t(b[1]) << 16) | (uint32_t(b[2]) << 8) | uint32_t(b[3]);
        return true;
    }
    bool readU64(uint64_t& v)
    {
        unsigned char b[8];
        f_.read(reinterpret_cast<char*>(b), 8);
        if (f_.gcount() != 8) return false;
        v = 0;
        for (int i = 0; i < 8; ++i) v = (v << 8) | b[i];
        return true;
    }

    std::ifstream f_;
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
// queue, which is fast enough to never be the bottleneck; a separate
// thread (captureLoop) drains it and does the actual (slower) JPEG
// compression at whatever pace it can sustain -- JPEG has no
// inter-frame dependencies and is fast enough that this should now
// rarely, if ever, need to actually drop anything.
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
// TranscodeJob / TranscodeQueue
//
// Handed from drainLoop to transcodeLoop once a recording's post-buffer
// has fully drained: capturePath is the finished .tmp.cap file to read
// back and transcode, outputPath is the final .mp4 to produce. Unlike
// FrameQueue, this is a plain unbounded queue with no drop-oldest
// behavior -- jobs are rare (one per finished recording, not one per
// frame) and each is independently valuable, so a backlog should just
// be worked through in order rather than discarded.
// ─────────────────────────────────────────────────────────────────────────────
struct TranscodeJob {
    std::string capturePath;
    std::string outputPath;
};

class TranscodeQueue {
public:
    void push(TranscodeJob job)
    {
        std::lock_guard<std::mutex> lk(mu_);
        jobs_.push_back(std::move(job));
        cv_.notify_one();
    }

    bool pop(TranscodeJob& out, const std::atomic<bool>& shutdown)
    {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait_for(lk, std::chrono::milliseconds(500),
                     [&] { return !jobs_.empty() || shutdown.load(); });
        if (jobs_.empty()) return false;
        out = std::move(jobs_.front());
        jobs_.pop_front();
        return true;
    }

    void wake() { cv_.notify_all(); }

private:
    std::mutex               mu_;
    std::condition_variable  cv_;
    std::deque<TranscodeJob> jobs_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Recorder
// ─────────────────────────────────────────────────────────────────────────────
// Idle -> Recording -> Draining -> Encoding -> Idle. Encoding is new:
// once a recording's post-buffer has drained there's no MP4 yet, only
// a finished capture file waiting for the transcode worker -- see the
// file's top comment.
enum class RecordState { Idle, Recording, Draining, Encoding };

struct RecorderStatus {
    std::string state;   // "idle" | "recording" | "draining" | "encoding"
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
        // actually keeping up on average (which it should, easily --
        // see FrameQueue's own comment).
        , frameQueue_(static_cast<size_t>(std::max(rawFps * 2, 30)))
    {
        readerThread_    = std::thread([this]{ readStream(); });
        captureThread_   = std::thread([this]{ captureLoop(); });
        transcodeThread_ = std::thread([this]{ transcodeLoop(); });
        drainThread_     = std::thread([this]{ drainLoop(); });
    }

    ~Recorder()
    {
        shutdown_ = true;
        drainCv_.notify_all();
        frameQueue_.wake();
        transcodeQueue_.wake();
        if (drainThread_.joinable())     drainThread_.join();
        if (readerThread_.joinable())    readerThread_.join();
        if (captureThread_.joinable())   captureThread_.join();
        if (transcodeThread_.joinable()) transcodeThread_.join();
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
            // Idle or Encoding both fall through to start a new
            // recording -- Encoding means a PREVIOUS recording is
            // still being transcoded in the background, which doesn't
            // block a new one from starting: its capture file is
            // entirely independent, and capture never waits on
            // transcode (see the file's top comment).
        }

        fs::create_directories(dir_);
        std::string stem    = fs::path(filename).stem().string();
        std::string outPath = (fs::path(dir_) / (stem + ".mp4")).string();
        std::string capPath = (fs::path(dir_) / (stem + ".tmp.cap")).string();

        if (fs::exists(outPath) || fs::exists(capPath))
            throw std::runtime_error("file already exists: " + outPath);

        // Unlike the old live-encode design, capture doesn't need an
        // H.264 encoder to be ready at all -- it just starts writing
        // whatever JPEG frames arrive next, so there's no equivalent
        // of the old "wait up to 10s for SPS/PPS" step here anymore.
        auto cw = std::make_unique<CaptureWriter>(capPath);

        auto pre = preBuf_.drain();
        std::cout << "[rec] Pre-buffer: flushing " << pre.size() << " frames\n";
        for (auto& f : pre)
            cw->appendFrame(f.jpeg, f.timestampUs, f.frameSeq);

        {
            std::lock_guard<std::mutex> lk(mu_);
            captureWriter_ = std::move(cw);
            current_       = outPath;
            state_         = RecordState::Recording;
            drainCmd_      = DrainCmd::Wait;
        }

        std::cout << "[rec] Recording started: " << outPath << "\n";
        return outPath;
    }

    std::string stop()
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (state_ == RecordState::Idle || state_ == RecordState::Encoding)
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
        else if (state_ == RecordState::Encoding) s = "encoding";
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
    // Dispatches one freshly JPEG-captured frame: always into the
    // rolling pre-buffer, and additionally into the current recording's
    // capture file if one is active.
    void onCapturedFrame(std::vector<uint8_t> jpeg, int64_t timestampUs, uint32_t frameSeq)
    {
        TP wallTime = std::chrono::system_clock::time_point(std::chrono::microseconds(timestampUs));

        std::lock_guard<std::mutex> lk(mu_);
        if ((state_ == RecordState::Recording || state_ == RecordState::Draining) && captureWriter_)
            captureWriter_->appendFrame(jpeg, timestampUs, frameSeq);
        preBuf_.push(std::move(jpeg), timestampUs, wallTime, frameSeq);
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

            // Post-buffer fully drained -- capture is done for this
            // recording. Unlike the old live-encode design, there is
            // no MP4 yet at this point, only a capture file of
            // buffered JPEG frames -- hand off to the transcode worker
            // rather than finalizing anything here directly.
            std::unique_ptr<CaptureWriter> cw;
            std::string outPath;
            {
                std::lock_guard<std::mutex> lk(mu_);
                cw        = std::move(captureWriter_);
                outPath   = current_;
                state_    = RecordState::Encoding;
                drainCmd_ = DrainCmd::Wait;
            }

            std::string capPath = cw->path();
            int nFrames = cw->close();
            auto recSecs = std::chrono::duration<double>(Clock::now() - stopTime_).count();
            std::cout << "[rec] " << toRFC3339(Clock::now())
                      << " capture closed: " << capPath
                      << " (" << nFrames << " frames, "
                      << std::fixed << std::setprecision(1) << recSecs
                      << "s since stop) -- queued for transcode\n";

            transcodeQueue_.push(TranscodeJob{ capPath, outPath });
        }
    }

    // ── JPEG capture encoder init (persistent, captureThread_ only) ──────────
    void initJpegEncoder()
    {
        const AVCodec* codec = avcodec_find_encoder(AV_CODEC_ID_MJPEG);
        if (!codec)
            throw std::runtime_error("MJPEG encoder not found");

        jpegCtx_.reset(avcodec_alloc_context3(codec));
        if (!jpegCtx_)
            throw std::runtime_error("avcodec_alloc_context3 (mjpeg) failed");

        jpegCtx_->width          = rawWidth_;
        jpegCtx_->height         = rawHeight_;
        jpegCtx_->time_base      = { 1, rawFps_ };
        jpegCtx_->pix_fmt        = AV_PIX_FMT_YUVJ420P; // MJPEG's native full-range format
        // Quality: the mjpeg encoder honors qscale/global_quality via
        // FF_QP2LAMBDA scaling; 2-5 is visually near-lossless while
        // still compressing far enough (~10-20x vs raw) that this can
        // never become the bottleneck real-time H.264 encoding was.
        jpegCtx_->flags         |= AV_CODEC_FLAG_QSCALE;
        jpegCtx_->global_quality = 4 * FF_QP2LAMBDA;

        int ret = avcodec_open2(jpegCtx_.get(), codec, nullptr);
        if (ret < 0) {
            char err[128]; av_strerror(ret, err, sizeof(err));
            throw std::runtime_error(std::string("avcodec_open2 (mjpeg): ") + err);
        }

        jpegFrame_.reset(av_frame_alloc());
        if (!jpegFrame_) throw std::runtime_error("av_frame_alloc (mjpeg) failed");
        jpegFrame_->format = AV_PIX_FMT_YUVJ420P;
        jpegFrame_->width  = rawWidth_;
        jpegFrame_->height = rawHeight_;
        if (av_frame_get_buffer(jpegFrame_.get(), 0) < 0)
            throw std::runtime_error("av_frame_get_buffer (mjpeg) failed");

        jpegPkt_.reset(av_packet_alloc());
        if (!jpegPkt_) throw std::runtime_error("av_packet_alloc (mjpeg) failed");

        std::cout << "[cap] JPEG capture ready: " << rawWidth_ << "x" << rawHeight_
                  << " @" << rawFps_ << "fps  stride=" << rawStride_ << "\n";
    }

    // ── JPEG-encode one raw YUV420 frame and dispatch it ──────────────────────
    void captureFrame(const std::vector<uint8_t>& yuv, int64_t timestampUs, uint32_t frameSeq)
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

        if (av_frame_make_writable(jpegFrame_.get()) < 0) return;

        // Copy each plane row-by-row, stripping stride padding
        for (int row = 0; row < rawHeight_; ++row)
            std::memcpy(jpegFrame_->data[0] + row * jpegFrame_->linesize[0],
                        yuv.data() + row * rawStride_, rawWidth_);
        for (int row = 0; row < uvHeight; ++row)
            std::memcpy(jpegFrame_->data[1] + row * jpegFrame_->linesize[1],
                        yuv.data() + yBytes + row * uvStride, uvWidth);
        for (int row = 0; row < uvHeight; ++row)
            std::memcpy(jpegFrame_->data[2] + row * jpegFrame_->linesize[2],
                        yuv.data() + yBytes + uvBytes + row * uvStride, uvWidth);

        jpegFrame_->pts = jpegPts_++;

        if (avcodec_send_frame(jpegCtx_.get(), jpegFrame_.get()) < 0) return;

        if (avcodec_receive_packet(jpegCtx_.get(), jpegPkt_.get()) == 0) {
            onCapturedFrame(std::vector<uint8_t>(jpegPkt_->data, jpegPkt_->data + jpegPkt_->size),
                            timestampUs, frameSeq);
            av_packet_unref(jpegPkt_.get());

            if (!firstFrameReady_.load()) {
                { std::lock_guard<std::mutex> lk(firstFrameMu_); firstFrameReady_ = true; }
                firstFrameCv_.notify_all();
            }
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

    // Drains frameQueue_ and JPEG-compresses each frame -- independent
    // of the receive loop's own pace, see FrameQueue's comment for why
    // these two are split across threads. Unlike H.264, JPEG has no
    // inter-frame dependencies and needs no real-time deadline pressure
    // relief, so unlike transcodeLoop below, this thread doesn't widen
    // its own CPU affinity -- it stays on the reserved core along with
    // the receive/TCP-control threads.
    void captureLoop()
    {
        try {
            initJpegEncoder();
        } catch (const std::exception& e) {
            std::cerr << "[cap] Encoder init failed: " << e.what() << "\n";
            return;
        }

        RawFrame f;
        while (!shutdown_) {
            if (!frameQueue_.pop(f, shutdown_)) continue; // timed out re-checking shutdown_, or woken by it
            captureFrame(f.yuv, f.timestampUs, f.frameSeq);
        }
    }

    // Widens this thread's own CPU affinity to every online core. Every
    // thread inherits main()'s core-2-only affinity mask (set before
    // any thread here existed) unless it explicitly changes its own --
    // that's fine, intentional even, for the receive/TCP-control/
    // capture threads, which stay lightweight and benefit from a
    // reserved, uncontended core. This is the one thread that actually
    // needs more: libx264 auto-detects its own internal thread count
    // from how many CPUs are visible to the calling thread's affinity
    // mask (av_cpu_count() reads sched_getaffinity()), so a
    // single-core mask would silently force single-threaded encoding
    // regardless of how many cores this Pi actually has.
    static void widenAffinityToAllCores(const char* logTag)
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        long nCpus = sysconf(_SC_NPROCESSORS_ONLN);
        if (nCpus < 1) nCpus = 1;
        for (long i = 0; i < nCpus; ++i) CPU_SET(static_cast<int>(i), &cpuset);
        if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0)
            std::cerr << logTag << " Warning: failed to widen thread's CPU affinity: " << strerror(errno) << "\n";
        else
            std::cout << logTag << " Thread using up to " << nCpus << " CPU(s)\n";
    }

    // Processes finished recordings' capture files into final MP4s, one
    // at a time -- unlike live encoding, this has no real-time deadline
    // to hit, so it can simply take as long as it needs. A transcode
    // job in progress when shutdown_ is set is allowed to finish (only
    // checked between jobs), matching readStream's own retry-loop
    // granularity.
    void transcodeLoop()
    {
        widenAffinityToAllCores("[xcode]");

        TranscodeJob job;
        while (!shutdown_) {
            if (!transcodeQueue_.pop(job, shutdown_)) continue;
            try {
                runTranscodeJob(job);
            } catch (const std::exception& e) {
                std::cerr << "[xcode] Job failed for " << job.capturePath << ": " << e.what() << "\n";
            }
            // Only reset to Idle if this job's output is still the
            // Recorder's "current" one -- a newer recording may have
            // already started (and even finished) while this job was
            // running, and its own state must not be clobbered here.
            std::lock_guard<std::mutex> lk(mu_);
            if (current_ == job.outputPath && state_ == RecordState::Encoding) {
                state_   = RecordState::Idle;
                current_ = "";
            }
        }
    }

    struct PendingNALU { std::vector<uint8_t> nalu; uint32_t dts; TP wallTime; uint32_t frameSeq; };

    // Reads capturePath sequentially (streaming, not loading it all
    // into memory at once -- a long recording could otherwise use a
    // lot of RAM even at JPEG's much smaller footprint than raw),
    // JPEG-decodes each frame, encodes it fresh with H.264, and muxes
    // the result into outputPath. A fresh encoder/decoder/scaler is
    // used for every job (opened and torn down here) rather than kept
    // as persistent Recorder state, since jobs are infrequent and this
    // keeps each one fully self-contained.
    void runTranscodeJob(const TranscodeJob& job)
    {
        CaptureReader reader(job.capturePath);
        if (!reader.ok())
            throw std::runtime_error("cannot open capture file: " + job.capturePath);

        // ── Fresh H.264 encoder for just this job ─────────────────────
        const AVCodec* h264Codec = avcodec_find_encoder(AV_CODEC_ID_H264);
        if (!h264Codec) throw std::runtime_error("H.264 encoder not found (need FFmpeg with libx264)");

        AVCodecContextPtr encCtx(avcodec_alloc_context3(h264Codec));
        if (!encCtx) throw std::runtime_error("avcodec_alloc_context3 (h264) failed");
        encCtx->width        = rawWidth_;
        encCtx->height       = rawHeight_;
        encCtx->time_base    = { 1, rawFps_ };
        encCtx->framerate    = { rawFps_, 1 };
        encCtx->pix_fmt      = AV_PIX_FMT_YUV420P;
        encCtx->gop_size     = rawFps_;
        encCtx->max_b_frames = 0;

        AVDictionary* opts = nullptr;
        av_dict_set(&opts, "preset", "ultrafast", 0);
        av_dict_set(&opts, "tune",   "zerolatency", 0);
        int ret = avcodec_open2(encCtx.get(), h264Codec, &opts);
        av_dict_free(&opts);
        if (ret < 0) {
            char err[128]; av_strerror(ret, err, sizeof(err));
            throw std::runtime_error(std::string("avcodec_open2 (h264): ") + err);
        }

        AVFramePtr encFrame(av_frame_alloc());
        if (!encFrame) throw std::runtime_error("av_frame_alloc (h264) failed");
        encFrame->format = AV_PIX_FMT_YUV420P;
        encFrame->width  = rawWidth_;
        encFrame->height = rawHeight_;
        if (av_frame_get_buffer(encFrame.get(), 0) < 0)
            throw std::runtime_error("av_frame_get_buffer (h264) failed");

        AVPacketPtr encPkt(av_packet_alloc());
        if (!encPkt) throw std::runtime_error("av_packet_alloc (h264) failed");

        // ── JPEG decoder, reused for every frame in this job ──────────
        const AVCodec* jpegDec = avcodec_find_decoder(AV_CODEC_ID_MJPEG);
        if (!jpegDec) throw std::runtime_error("MJPEG decoder not found");
        AVCodecContextPtr decCtx(avcodec_alloc_context3(jpegDec));
        if (!decCtx) throw std::runtime_error("avcodec_alloc_context3 (mjpeg dec) failed");
        if (avcodec_open2(decCtx.get(), jpegDec, nullptr) < 0)
            throw std::runtime_error("avcodec_open2 (mjpeg dec) failed");

        AVFramePtr decFrame(av_frame_alloc());
        if (!decFrame) throw std::runtime_error("av_frame_alloc (mjpeg dec) failed");
        AVPacketPtr decPkt(av_packet_alloc());
        if (!decPkt) throw std::runtime_error("av_packet_alloc (mjpeg dec) failed");

        // Built lazily once the JPEG decoder reports its actual output
        // format/size (MJPEG typically decodes to YUVJ420P); converts
        // explicitly to plain YUV420P rather than relying on the
        // encoder silently accepting a full-range format it wasn't
        // configured for.
        SwsContextPtr sws;

        std::vector<uint8_t> sps, pps;
        std::unique_ptr<MP4Muxer> muxer;
        std::vector<PendingNALU> pending; // buffered until sps+pps are both known

        auto dtsFor = [](int64_t timestampUs) -> uint32_t {
            return static_cast<uint32_t>((timestampUs * 90LL) / 1000LL);
        };

        auto dispatchNALU = [&](const uint8_t* naluData, size_t len, uint32_t dts, TP wallTime, uint32_t frameSeq) {
            if (len == 0) return;
            uint8_t nalType = naluData[0] & 0x1F;
            std::vector<uint8_t> v(naluData, naluData + len);
            if (nalType == 7) sps = v;
            else if (nalType == 8) pps = v;

            if (!muxer && !sps.empty() && !pps.empty()) {
                muxer = std::make_unique<MP4Muxer>(job.outputPath, sps, pps);
                for (auto& pn : pending)
                    muxer->writeNALU(pn.nalu, pn.dts, pn.wallTime, pn.frameSeq);
                pending.clear();
            }

            if (muxer) muxer->writeNALU(v, dts, wallTime, frameSeq);
            else       pending.push_back({ std::move(v), dts, wallTime, frameSeq });
        };

        // Same Annex-B/AVCC-agnostic NALU splitting picam-recorder has
        // always used for libx264's output.
        auto dispatchEncodedPacket = [&](AVPacket* pkt, uint32_t dts, TP wallTime, uint32_t frameSeq) {
            const uint8_t* p   = pkt->data;
            const uint8_t* end = pkt->data + pkt->size;
            bool annexB = (pkt->size >= 4 && p[0] == 0 && p[1] == 0 && (p[2] == 1 || (p[2] == 0 && p[3] == 1)));

            if (annexB) {
                const uint8_t* naluStart = nullptr;
                while (p < end) {
                    bool sc3 = (p + 3 <= end && p[0]==0 && p[1]==0 && p[2]==1);
                    bool sc4 = (p + 4 <= end && p[0]==0 && p[1]==0 && p[2]==0 && p[3]==1);
                    if (sc3 || sc4) {
                        if (naluStart) {
                            const uint8_t* naluEnd = p;
                            while (naluEnd > naluStart && *(naluEnd-1) == 0) --naluEnd;
                            dispatchNALU(naluStart, naluEnd - naluStart, dts, wallTime, frameSeq);
                        }
                        p += sc4 ? 4 : 3;
                        naluStart = p;
                    } else {
                        ++p;
                    }
                }
                if (naluStart && naluStart < end)
                    dispatchNALU(naluStart, end - naluStart, dts, wallTime, frameSeq);
            } else {
                while (p + 4 <= end) {
                    uint32_t naluLen = (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) |
                                       (uint32_t(p[2]) <<  8) |  uint32_t(p[3]);
                    p += 4;
                    if (p + naluLen > end) break;
                    dispatchNALU(p, naluLen, dts, wallTime, frameSeq);
                    p += naluLen;
                }
            }
        };

        int64_t       encPts = 0;
        size_t        frameCount = 0;
        CapturedFrame cf;
        CapturedFrame lastFrame{};
        bool          haveAny = false;

        while (reader.readNext(cf)) {
            ++frameCount;
            lastFrame = cf;
            haveAny   = true;

            av_packet_unref(decPkt.get());
            decPkt->data = cf.jpeg.data();
            decPkt->size = static_cast<int>(cf.jpeg.size());
            if (avcodec_send_packet(decCtx.get(), decPkt.get()) < 0) continue;

            while (avcodec_receive_frame(decCtx.get(), decFrame.get()) == 0) {
                if (!sws) {
                    sws.reset(sws_getContext(decFrame->width, decFrame->height, static_cast<AVPixelFormat>(decFrame->format),
                                              rawWidth_, rawHeight_, AV_PIX_FMT_YUV420P,
                                              SWS_BILINEAR, nullptr, nullptr, nullptr));
                    if (!sws) throw std::runtime_error("sws_getContext failed");
                }

                if (av_frame_make_writable(encFrame.get()) == 0) {
                    sws_scale(sws.get(), decFrame->data, decFrame->linesize, 0, decFrame->height,
                              encFrame->data, encFrame->linesize);

                    encFrame->pts = encPts++;
                    TP       wallTime = std::chrono::system_clock::time_point(std::chrono::microseconds(cf.timestampUs));
                    uint32_t dts      = dtsFor(cf.timestampUs);

                    if (avcodec_send_frame(encCtx.get(), encFrame.get()) == 0) {
                        while (avcodec_receive_packet(encCtx.get(), encPkt.get()) == 0) {
                            dispatchEncodedPacket(encPkt.get(), dts, wallTime, cf.frameSeq);
                            av_packet_unref(encPkt.get());
                        }
                    }
                }
                av_frame_unref(decFrame.get());
            }
        }

        // Flush the encoder for any internally-buffered output.
        avcodec_send_frame(encCtx.get(), nullptr);
        while (avcodec_receive_packet(encCtx.get(), encPkt.get()) == 0) {
            TP       wallTime = haveAny ? std::chrono::system_clock::time_point(std::chrono::microseconds(lastFrame.timestampUs)) : Clock::now();
            uint32_t dts      = haveAny ? dtsFor(lastFrame.timestampUs) : 0;
            uint32_t seq      = haveAny ? lastFrame.frameSeq : 0;
            dispatchEncodedPacket(encPkt.get(), dts, wallTime, seq);
            av_packet_unref(encPkt.get());
        }

        std::error_code ec;
        if (!muxer) {
            std::cerr << "[xcode] " << job.capturePath << ": no frames produced a keyframe -- discarding, no MP4 written\n";
            fs::remove(job.capturePath, ec);
            return;
        }

        int n = muxer->close();
        fs::remove(job.capturePath, ec);
        std::cout << "[xcode] " << toRFC3339(Clock::now()) << " transcoded: " << job.outputPath
                  << " (" << n << " NALUs, " << std::fixed << std::setprecision(1)
                  << fileSizeMiB(job.outputPath) << " MiB, from " << frameCount << " captured frames)\n";
    }

    // ── members ──────────────────────────────────────────────────────────────
    std::mutex                     mu_;
    RecordState                    state_   = RecordState::Idle;
    std::string                    current_;
    std::unique_ptr<CaptureWriter> captureWriter_;
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

    std::thread                    readerThread_;
    std::thread                    captureThread_;
    std::thread                    transcodeThread_;
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

    // JPEG capture encoder (used only from captureThread_)
    AVCodecContextPtr              jpegCtx_;
    AVFramePtr                     jpegFrame_;
    AVPacketPtr                    jpegPkt_;
    int64_t                        jpegPts_ = 0;

    TranscodeQueue                 transcodeQueue_; // finished recordings awaiting transcode -- see its own comment
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

    // ── 0. Pin this process to CPU core 2 ────────────────────────────────────
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(2, &cpuset);
        if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0)
            std::cerr << "[main] Warning: failed to set CPU affinity: " << strerror(errno) << "\n";
        else
            std::cout << "[main] Pinned to CPU core 2\n";
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
