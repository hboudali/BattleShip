# Battleship

Online two-player Battleship. Each player uses their own phone: one creates a
game and shares the short code, the other joins with it.

## How it works

- Static site, no build step: `index.html`, `css/`, `js/`.
- Players sync through free public MQTT brokers (Eclipse, EMQX, HiveMQ,
  Mosquitto) over secure WebSockets, on a topic named after the game code.
  Each phone uses all of them at once, so a network that blocks one broker's
  port doesn't stop the game. No accounts, server or
  database needed. The client library (mqtt.js) is bundled in `js/vendor/`.
- Each device keeps its own fleet private and resolves incoming shots itself;
  ship positions are only revealed when the game ends.
- Game state is saved in `localStorage`, so a locked phone or page reload
  picks the game back up.

## Configuration

The broker list and topic prefix live in `js/config.js`.

## Run locally

```sh
python3 -m http.server 8000
# open http://localhost:8000
```

## Deploy

Enable GitHub Pages (Settings → Pages → Deploy from branch → `main` / root).
