CXX      := g++
CXXFLAGS := -std=c++17 -O2 -pthread -Wall -Wextra
CXXFLAGS += $(shell pkg-config --cflags libavformat libavcodec libavutil)
LDFLAGS  := $(shell pkg-config --libs   libavformat libavcodec libavutil)

all: picam-recorder

picam-recorder: src/main.cpp
	$(CXX) $(CXXFLAGS) $< $(LDFLAGS) -o $@

install:
	install -D -m 0755 picam-recorder $(DESTDIR)/usr/local/bin/picam-recorder
	install -D -m 0644 debian/recorder.ini $(DESTDIR)/etc/picam-recorder/recorder.ini
	install -D -m 0644 debian/picam-recorder.service $(DESTDIR)/lib/systemd/system/picam-recorder.service
	install -d -m 0755 $(DESTDIR)/var/lib/picam-recorder

clean:
	rm -f picam-recorder
