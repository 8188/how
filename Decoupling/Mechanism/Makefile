CXX = g++
CXXFLAGS = -pthread -std=c++17 -I..
LIBS = -lredis++ -lhiredis -lpaho-mqttpp3
MQTT_LIB = $(shell ./detect_mqtt.sh)

all: utils

utils: utils.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS) $(MQTT_LIB)

debug: CXXFLAGS += -g -Wall -Wextra
debug: utils

opt: CXXFLAGS += -O3
opt: utils

clean:
	rm -f utils

.PHONY: all debug opt clean