/**
 * picam_recorder — records raw YUV420 frames from picam-raw UDP stream to MP4
 *
 * Receives uncompressed YUV420 frames via picam-raw's UDP chunk protocol,
 * encodes them to H.264 with FFmpeg (libx264), and muxes to MP4 files with
 * pre-roll and post-roll buffering.
 *
 * Dependencies:
 *   - FFmpeg libs: libavformat, libavcodec, libavutil  (encoding + MP4 muxing)
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
}

// ── stdlib ────────────────────────────────────────────────────────────────────
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

static Config loadIni(const std::string& path)
{
    std::ifstream f(path);
    if (!f)
        throw std::runtime_error("cannot open config file: " + path);

    Config cfg;
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
// BufferedNALU
// ─────────────────────────────────────────────────────────────────────────────
struct BufferedNALU {
    std::vector<uint8_t> nalu;
    uint32_t             dts;
    TP                   wallTime;
    uint32_t             frameSeq;
};

// ─────────────────────────────────────────────────────────────────────────────
// RollingBuffer
// ─────────────────────────────────────────────────────────────────────────────
class RollingBuffer {
public:
    explicit RollingBuffer(double secs) : secs_(secs) {}

    void push(std::vector<uint8_t> nalu, uint32_t dts, TP wallTime, uint32_t frameSeq)
    {
        std::lock_guard<std::mutex> lk(mu_);
        frames_.push_back({ std::move(nalu), dts, wallTime, frameSeq });
        auto cutoff = Clock::now() -
                      std::chrono::duration_cast<Clock::duration>(
                          std::chrono::duration<double>(secs_));
        while (!frames_.empty() && frames_.front().wallTime < cutoff)
            frames_.pop_front();
    }

    std::deque<BufferedNALU> drain()
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::deque<BufferedNALU> out;
        std::swap(out, frames_);
        return out;
    }

private:
    std::mutex               mu_;
    std::deque<BufferedNALU> frames_;
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

        static const uint8_t kStartCode[4] = {0x00, 0x00, 0x00, 0x01};
        std::vector<uint8_t> annexb;
        annexb.reserve(4 + nalu.size());
        annexb.insert(annexb.end(), kStartCode, kStartCode + 4);
        annexb.insert(annexb.end(), nalu.begin(), nalu.end());

        av_packet_unref(pkt_);
        pkt_->data         = annexb.data();
        pkt_->size         = static_cast<int>(annexb.size());
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
// Recorder
// ─────────────────────────────────────────────────────────────────────────────
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
    {
        readerThread_ = std::thread([this]{ readStream(); });
        drainThread_  = std::thread([this]{ drainLoop(); });
    }

    ~Recorder()
    {
        shutdown_ = true;
        drainCv_.notify_all();
        if (drainThread_.joinable())  drainThread_.join();
        if (readerThread_.joinable()) readerThread_.join();
        if (encFrame_) av_frame_free(&encFrame_);
        if (encPkt_)   av_packet_free(&encPkt_);
        if (encCtx_)   avcodec_free_context(&encCtx_);
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
        }

        // ── Idle: wait for SPS/PPS from first encoded IDR frame ───────────────
        {
            std::unique_lock<std::mutex> lk(spsPpsMu_);
            bool ready = spsPpsCv_.wait_for(lk, std::chrono::seconds(10),
                             [this]{ return spsPpsReady_.load(); });
            if (!ready)
                throw std::runtime_error("stream not ready — no SPS/PPS received after 10s");
        }

        {
            std::lock_guard<std::mutex> lk(mu_);

            fs::create_directories(dir_);
            std::string stem    = fs::path(filename).stem().string();
            std::string outPath = (fs::path(dir_) / (stem + ".mp4")).string();

            if (fs::exists(outPath))
                throw std::runtime_error("file already exists: " + outPath);

            muxer_    = std::make_unique<MP4Muxer>(outPath, sps_, pps_);
            current_  = outPath;
            state_    = RecordState::Recording;
            drainCmd_ = DrainCmd::Wait;

            auto pre = preBuf_.drain();
            std::cout << "[rec] Pre-buffer: flushing " << pre.size() << " frames\n";
            for (auto& f : pre)
                muxer_->writeNALU(f.nalu, f.dts, f.wallTime, f.frameSeq);

            std::cout << "[rec] Recording started: " << outPath << "\n";
            return outPath;
        }
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
        if (state_ == RecordState::Recording) s = "recording";
        else if (state_ == RecordState::Draining) s = "draining";
        return { s, current_ };
    }

    double postBufferSecs() const { return postBufferSecs_; }
    const std::string& dir() const { return dir_; }

    bool waitForStream(int timeoutSecs = 60)
    {
        std::unique_lock<std::mutex> lk(spsPpsMu_);
        return spsPpsCv_.wait_for(lk,
            std::chrono::seconds(timeoutSecs),
            [this]{ return spsPpsReady_.load(); });
    }

    bool streamReady() const { return spsPpsReady_.load(); }

private:
    void writeNALU(std::vector<uint8_t> nalu, uint32_t dts, TP wallTime, uint32_t frameSeq)
    {
        preBuf_.push(nalu, dts, wallTime, frameSeq);
        std::lock_guard<std::mutex> lk(mu_);
        if ((state_ == RecordState::Recording || state_ == RecordState::Draining) && muxer_)
            muxer_->writeNALU(nalu, dts, wallTime, frameSeq);
    }

    void drainLoop()
    {
        while (!shutdown_) {
            std::string path;

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
                path = current_;
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

            auto closeTime = Clock::now();
            auto recSecs   = std::chrono::duration<double>(closeTime - stopTime_).count();

            std::unique_ptr<MP4Muxer> m;
            {
                std::lock_guard<std::mutex> lk(mu_);
                m         = std::move(muxer_);
                state_    = RecordState::Idle;
                current_  = "";
                drainCmd_ = DrainCmd::Wait;
            }

            int n = m->close();
            std::cout << "[rec] " << toRFC3339(closeTime)
                      << " file closed: " << path
                      << " (" << n << " NALUs, "
                      << std::fixed << std::setprecision(1)
                      << fileSizeMiB(path) << " MiB, "
                      << recSecs << "s since stop)\n";
        }
    }

    // ── H.264 encoder init ────────────────────────────────────────────────────
    void initEncoder()
    {
        const AVCodec* codec = avcodec_find_encoder(AV_CODEC_ID_H264);
        if (!codec)
            throw std::runtime_error("H.264 encoder not found (need FFmpeg with libx264)");

        encCtx_ = avcodec_alloc_context3(codec);
        if (!encCtx_)
            throw std::runtime_error("avcodec_alloc_context3 failed");

        encCtx_->width        = rawWidth_;
        encCtx_->height       = rawHeight_;
        encCtx_->time_base    = { 1, rawFps_ };
        encCtx_->framerate    = { rawFps_, 1 };
        encCtx_->pix_fmt      = AV_PIX_FMT_YUV420P;
        encCtx_->gop_size     = rawFps_;   // IDR every second
        encCtx_->max_b_frames = 0;

        AVDictionary* opts = nullptr;
        av_dict_set(&opts, "preset", "ultrafast", 0);
        av_dict_set(&opts, "tune",   "zerolatency", 0);

        int ret = avcodec_open2(encCtx_, codec, &opts);
        av_dict_free(&opts);
        if (ret < 0) {
            char err[128]; av_strerror(ret, err, sizeof(err));
            throw std::runtime_error(std::string("avcodec_open2: ") + err);
        }

        encFrame_ = av_frame_alloc();
        if (!encFrame_) throw std::runtime_error("av_frame_alloc failed");
        encFrame_->format = AV_PIX_FMT_YUV420P;
        encFrame_->width  = rawWidth_;
        encFrame_->height = rawHeight_;
        if (av_frame_get_buffer(encFrame_, 0) < 0)
            throw std::runtime_error("av_frame_get_buffer failed");

        encPkt_ = av_packet_alloc();
        if (!encPkt_) throw std::runtime_error("av_packet_alloc for encoder failed");

        std::cout << "[raw] Encoder ready: H.264 " << rawWidth_ << "x" << rawHeight_
                  << " @" << rawFps_ << "fps  stride=" << rawStride_ << "\n";
    }

    // ── Encode one YUV420 frame and dispatch resulting NALUs ─────────────────
    void encodeFrame(const std::vector<uint8_t>& yuv, int64_t timestampUs, uint32_t frameSeq)
    {
        const int    uvStride = rawStride_ / 2;
        const int    uvHeight = rawHeight_ / 2;
        const int    uvWidth  = rawWidth_  / 2;
        const size_t yBytes   = static_cast<size_t>(rawStride_) * rawHeight_;
        const size_t uvBytes  = static_cast<size_t>(uvStride) * uvHeight;

        if (yuv.size() < yBytes + 2 * uvBytes) {
            std::cerr << "[raw] Frame size " << yuv.size()
                      << " < expected " << (yBytes + 2 * uvBytes) << " — skipping\n";
            return;
        }

        if (av_frame_make_writable(encFrame_) < 0) return;

        // Copy each plane row-by-row, stripping stride padding
        for (int row = 0; row < rawHeight_; ++row)
            std::memcpy(encFrame_->data[0] + row * encFrame_->linesize[0],
                        yuv.data() + row * rawStride_, rawWidth_);
        for (int row = 0; row < uvHeight; ++row)
            std::memcpy(encFrame_->data[1] + row * encFrame_->linesize[1],
                        yuv.data() + yBytes + row * uvStride, uvWidth);
        for (int row = 0; row < uvHeight; ++row)
            std::memcpy(encFrame_->data[2] + row * encFrame_->linesize[2],
                        yuv.data() + yBytes + uvBytes + row * uvStride, uvWidth);

        encFrame_->pts = encPts_++;

        TP       wallTime = std::chrono::system_clock::time_point(
                                std::chrono::microseconds(timestampUs));
        uint32_t dts      = static_cast<uint32_t>((timestampUs * 90LL) / 1000LL);

        if (avcodec_send_frame(encCtx_, encFrame_) < 0) return;

        while (avcodec_receive_packet(encCtx_, encPkt_) == 0) {
            dispatchEncodedPacket(encPkt_, dts, wallTime, frameSeq);
            av_packet_unref(encPkt_);
        }
    }

    // ── Split an encoded packet into NALUs and dispatch each one ─────────────
    void dispatchEncodedPacket(AVPacket* pkt, uint32_t dts, TP wallTime, uint32_t frameSeq)
    {
        const uint8_t* p   = pkt->data;
        const uint8_t* end = pkt->data + pkt->size;

        // libx264 without GLOBAL_HEADER outputs Annex-B
        bool annexB = (pkt->size >= 4 &&
                       p[0] == 0 && p[1] == 0 &&
                       (p[2] == 1 || (p[2] == 0 && p[3] == 1)));

        auto dispatchNALU = [&](const uint8_t* nalu, size_t len) {
            if (len == 0) return;
            uint8_t nalType = nalu[0] & 0x1F;
            std::vector<uint8_t> v(nalu, nalu + len);
            {
                std::lock_guard<std::mutex> lk(mu_);
                if (nalType == 7) sps_ = v;
                else if (nalType == 8) pps_ = v;
            }
            // Signal ready once both SPS and PPS are available
            if (nalType == 7 || nalType == 8) {
                bool hasBoth;
                { std::lock_guard<std::mutex> lk(mu_); hasBoth = !sps_.empty() && !pps_.empty(); }
                if (hasBoth) {
                    { std::lock_guard<std::mutex> slk(spsPpsMu_); spsPpsReady_ = true; }
                    spsPpsCv_.notify_all();
                }
            }
            writeNALU(std::move(v), dts, wallTime, frameSeq);
        };

        if (annexB) {
            const uint8_t* naluStart = nullptr;
            while (p < end) {
                bool sc3 = (p + 3 <= end && p[0]==0 && p[1]==0 && p[2]==1);
                bool sc4 = (p + 4 <= end && p[0]==0 && p[1]==0 && p[2]==0 && p[3]==1);
                if (sc3 || sc4) {
                    if (naluStart) {
                        const uint8_t* naluEnd = p;
                        while (naluEnd > naluStart && *(naluEnd-1) == 0) --naluEnd;
                        dispatchNALU(naluStart, naluEnd - naluStart);
                    }
                    p += sc4 ? 4 : 3;
                    naluStart = p;
                } else {
                    ++p;
                }
            }
            if (naluStart && naluStart < end)
                dispatchNALU(naluStart, end - naluStart);
        } else {
            // AVCC: 4-byte big-endian length prefix
            while (p + 4 <= end) {
                uint32_t naluLen = (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) |
                                   (uint32_t(p[2]) <<  8) |  uint32_t(p[3]);
                p += 4;
                if (p + naluLen > end) break;
                dispatchNALU(p, naluLen);
                p += naluLen;
            }
        }
    }

    // ── Connect to picam-raw UDP stream, reassemble frames, encode ────────────
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
                // All chunks arrived — reassemble frame and encode
                std::vector<uint8_t> yuv;
                size_t totalSize = 0;
                for (auto& c : pf.chunks) totalSize += c.size();
                yuv.reserve(totalSize);
                for (auto& c : pf.chunks)
                    yuv.insert(yuv.end(), c.begin(), c.end());

                int64_t  ts  = pf.timestampUs;
                uint32_t seq = frameSeq;
                pending.erase(it);
                encodeFrame(yuv, ts, seq);
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
        try {
            initEncoder();
        } catch (const std::exception& e) {
            std::cerr << "[raw] Encoder init failed: " << e.what() << "\n";
            return;
        }

        while (!shutdown_) {
            try { connectRaw(); }
            catch (const std::exception& e) {
                std::cerr << "[raw] Error: " << e.what() << " — retrying in 3s\n";
            }
            if (!shutdown_)
                std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }

    // ── members ──────────────────────────────────────────────────────────────
    std::mutex                mu_;
    RecordState               state_   = RecordState::Idle;
    std::string               current_;
    std::unique_ptr<MP4Muxer> muxer_;
    std::string               rawHost_;
    int                       rawPort_;
    int                       rawWidth_;
    int                       rawHeight_;
    int                       rawFps_;
    int                       rawStride_;
    std::string               dir_;
    std::vector<uint8_t>      sps_, pps_;
    RollingBuffer             preBuf_;
    double                    preBufferSecs_;
    double                    postBufferSecs_;

    std::thread               readerThread_;
    std::thread               drainThread_;
    std::atomic<bool>         shutdown_{ false };

    std::condition_variable   drainCv_;
    TP                        stopTime_;

    enum class DrainCmd { Wait, Stop, Resume };
    DrainCmd                  drainCmd_ = DrainCmd::Wait;

    std::mutex                spsPpsMu_;
    std::atomic<bool>         spsPpsReady_{ false };
    std::condition_variable   spsPpsCv_;

    // H.264 encoder (used only from readerThread_)
    AVCodecContext*           encCtx_   = nullptr;
    AVFrame*                  encFrame_ = nullptr;
    AVPacket*                 encPkt_   = nullptr;
    int64_t                   encPts_   = 0;
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
