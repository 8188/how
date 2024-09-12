#!/bin/bash

source .env

if [[ "$MQTT_ADDRESS" == wss:* || "$MQTT_ADDRESS" == mqtts:* ]]; then
    echo "-lpaho-mqtt3as"
else
    echo "-lpaho-mqtt3a"
fi
