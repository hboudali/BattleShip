# Battleship

Online two-player Battleship. Each player uses their own phone: one creates a
game and shares the short code, the other joins with it.

## How it works

- Static site, no build step: `index.html`, `css/`, `js/`.
- Players sync through a [Supabase Realtime](https://supabase.com/docs/guides/realtime)
  channel named after the game code (presence + broadcast). No database tables.
- Each device keeps its own fleet private and resolves incoming shots itself;
  ship positions are only revealed when the game ends.
- Game state is saved in `localStorage`, so a locked phone or page reload
  picks the game back up.

## Configuration

Supabase URL and anon key live in `js/config.js`.

## Run locally

```sh
python3 -m http.server 8000
# open http://localhost:8000
```

## Deploy

Enable GitHub Pages (Settings → Pages → Deploy from branch → `main` / root).
