/**
 * Connection Configuration
 *
 * Players meet on free public MQTT brokers over secure WebSockets, so no
 * account or server is needed. Each phone connects to all of these at once
 * and sends every message through each one, so the game works as long as
 * both phones can reach at least one of them (some networks block the
 * non-standard ports; Eclipse uses the normal HTTPS port 443).
 */
export const MQTT_CONFIG = {
  BROKERS: [
    'wss://mqtt.eclipseprojects.io:443/mqtt',
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
    'wss://test.mosquitto.org:8081/mqtt'
  ],
  TOPIC_PREFIX: 'hboudali-battleship/v1'
};
