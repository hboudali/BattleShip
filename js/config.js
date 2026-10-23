/**
 * Connection Configuration
 *
 * Players meet on a free public MQTT broker over secure WebSockets, so no
 * account or server is needed. Both players must use the same broker and
 * topic prefix. Alternatives: wss://broker.emqx.io:8084/mqtt or
 * wss://test.mosquitto.org:8081.
 */
export const MQTT_CONFIG = {
  URL: 'wss://broker.hivemq.com:8884/mqtt',
  TOPIC_PREFIX: 'hboudali-battleship/v1'
};
