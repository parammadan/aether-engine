# Producing the demo assets

Three showcase assets feed the top of the README. Here's exactly how to make each.

## 1. `docs/demo.gif` — the dashboard chaos harness (highest impact)

A ~30-second GIF is the single best way to make the system legible at a glance.

**Record it:**
1. `cargo run -p dashboard` and open <http://127.0.0.1:8080>.
2. Let the live query stream run for a few seconds (throughput/coverage ticking).
3. Click **kill −9** on a shard leader. Watch: coverage drops to partial, the event log
   shows the kill, the follower is promoted (~6 s), coverage returns — and the query stream
   never errors.
4. Click **Add follower** to show a fresh node registering and catching up.
5. Type a question in the NLQ bar (e.g. "how many flights are there?") to show the agent loop.

**Capture → GIF:**
- Easiest: [Kap](https://getkap.co) (free, macOS) — record the browser region, export as GIF.
- Or QuickTime screen recording → convert:
  ```bash
  # trim/scale and convert a .mov to an optimized GIF
  ffmpeg -i recording.mov -vf "fps=12,scale=1000:-1:flags=lanczos" -c:v gif docs/demo.gif
  # or higher quality with gifski:
  ffmpeg -i recording.mov -vf "fps=12,scale=1000:-1" frames/%04d.png && gifski -o docs/demo.gif frames/*.png
  ```
- Aim for < ~8 MB so it loads fast on GitHub.

Save it as `docs/demo.gif`; the README already references it.

## 2. Terminal cast — the chaos story on the CLI (asciinema)

```bash
brew install asciinema
asciinema rec aether.cast -c ./scripts/demo.sh   # runs the narrated demo
asciinema upload aether.cast                       # prints a shareable URL
```

Paste the returned URL into the README's demo links line (replace `REPLACE_ME`).

## 3. Live demo — public clickable dashboard (Fly.io)

```bash
brew install flyctl
fly auth login
fly launch --no-deploy    # uses the existing fly.toml (app: aether-demo)
fly deploy
fly open                  # your URL, e.g. https://aether-demo.fly.dev
```

The README's **Live demo** link points at `https://aether-demo.fly.dev` — update it if you
choose a different app name. It's a scale-to-zero VM, so it's ~free when idle and wakes on
the next request.
