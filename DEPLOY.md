# Deploying to production (wr-mac-studio-1)

Production runs on **wr-mac-studio-1**, a Mac Studio reachable over Tailscale
SSH (alias `wr-mac-studio-1` in `~/.ssh/config`), as the `dev` user at
`~/apps/wm-land-screener`. It is fully set up already — credentials, Python
deps, and Land Screener's parcel caches all exist there. These steps are the
**regular update cycle**, not a first-time setup.

**This replaced deploying to an EC2 box on 2026-08-07.** That process (systemd,
`ubuntu@44.213.16.32:/opt/wm-land-screener`, the `live` git remote) is retired —
don't follow it or push to `live` without being told to revive it explicitly.
`git remote -v` still lists `live`; that's leftover, not a live target.

## How the app runs there

A system LaunchDaemon, not a plain terminal session:

- Label: `co.wakerobin.wm-land-screener`
- Plist: `/Library/LaunchDaemons/co.wakerobin.wm-land-screener.plist`
- Runs: `.venv/bin/streamlit run src/app_shell.py --server.port=8501 --server.address=127.0.0.1`
- `KeepAlive=true`, `RunAtLoad=true` — it restarts itself if it dies or the
  Studio reboots. **`WR_DEPLOY_ENV=production`** is set in its environment,
  which hides the local econ-dev scan/curation UI (same gate as the old EC2
  setup) — curation still happens from a real local machine, not this box.
- Logs: `~/apps/wm-land-screener/logs/{stdout,stderr,streamlit}.log`

**Restart command — use this, not a manual `kill`:**
```bash
ssh wr-mac-studio-1 'sudo launchctl kickstart -k system/co.wakerobin.wm-land-screener'
```
`KeepAlive` will respawn a killed process on its own, usually within a couple
of seconds — faster than a manual relaunch can react. Manually killing the PID
and starting a replacement by hand races the daemon's own respawn, produces a
confusing "Port 8501 is not available" collision in stderr.log when both try to
bind at once, and leaves you unsure afterward which process is actually
launchd's supervised one. Check with `launchctl print
system/co.wakerobin.wm-land-screener` (look at its `pid` field) if it's ever
unclear which process is running.

## Regular update — code changed, no new county/data source

```bash
ssh wr-mac-studio-1 'cd ~/apps/wm-land-screener && git status --short && echo --- && git pull'
# (this is exactly what the sync-studio skill does)
ssh wr-mac-studio-1 'sudo launchctl kickstart -k system/co.wakerobin.wm-land-screener'
```
Then verify (see below). Skip the cache-refresh section unless the change
touched `config.py: MARKET_COUNTIES` or added/changed a market data source.

## Market Feasibility cache refresh — ONLY when MARKET_COUNTIES or a data source changed

**A code deploy alone does not add a new county to the live site.** Every
Market Feasibility loader short-circuits to its gitignored `data/raw/market_*`
cache, and nothing calls `refresh=True` automatically. Worse: `_market_data()`
in `render.py` is `@st.cache_data` with no TTL, so even after refreshing the
on-disk files, the **already-running process keeps serving what it loaded at
startup** — refreshing the cache and restarting the service are one action,
not two. Do the refresh, THEN restart, in that order (restarting a cold
process before refreshing just means it caches the stale data instead).

```bash
ssh wr-mac-studio-1 'cd ~/apps/wm-land-screener/src && \
  ../.venv/bin/python3 -m market.demographics --refresh && \
  ../.venv/bin/python3 -c "import sys; sys.path.insert(0,\".\"); \
    from market.demographics import load_municipal_metrics; load_municipal_metrics(refresh=True)" && \
  ../.venv/bin/python3 -m market.boundaries   --refresh && \
  ../.venv/bin/python3 -m market.fred         --refresh && \
  ../.venv/bin/python3 -m market.zillow       --refresh && \
  ../.venv/bin/python3 -m market.census_bps   --refresh && \
  ../.venv/bin/python3 -m market.lodes        --refresh'
```
Two gotchas, both real, both hit while doing this on 2026-08-07:
- `market.demographics --refresh` only rebuilds the **county** frame; the
  municipal frame needs the separate explicit call shown above.
- A FRED series can come back **empty** on a transient fetch failure and still
  get written to the cache. After refreshing, confirm every county has all
  three series before trusting it:
```bash
ssh wr-mac-studio-1 'cd ~/apps/wm-land-screener/src && ../.venv/bin/python3 -c "
import sys; sys.path.insert(0,\".\")
from market import fred as F
d = F.load_fred_data()
for k,v in sorted(d[\"counties\"].items()):
    print(k, len(v.get(\"unemployment\") or []), len(v.get(\"permits\") or []), len(v.get(\"hpi\") or []))
"'
```
Any county showing a `0` means re-run the FRED refresh for that layer.

Then restart:
```bash
ssh wr-mac-studio-1 'sudo launchctl kickstart -k system/co.wakerobin.wm-land-screener'
```

## Verify

```bash
ssh wr-mac-studio-1 'curl -s http://127.0.0.1:8501/_stcore/health'   # expect: ok
ssh wr-mac-studio-1 'launchctl print system/co.wakerobin.wm-land-screener | grep -E "state|pid|last exit"'
```
If checking a market-feasibility change specifically, also confirm the county
list directly rather than trusting the process is fresh:
```bash
ssh wr-mac-studio-1 'cd ~/apps/wm-land-screener/src && ../.venv/bin/python3 -c "
import sys; sys.path.insert(0,\".\")
from market import render as R
_, needs, *_ = R._market_data()
print(sorted(needs[\"key\"].tolist()))
"'
```

## Rollback

```bash
ssh wr-mac-studio-1 'cd ~/apps/wm-land-screener && git log --oneline -5'   # find the commit to roll back to
ssh wr-mac-studio-1 'cd ~/apps/wm-land-screener && git checkout <previous-commit>'
ssh wr-mac-studio-1 'sudo launchctl kickstart -k system/co.wakerobin.wm-land-screener'
```

## One-time setup (already done on wr-mac-studio-1 — reference only, e.g. for a new target)

These are NOT part of the regular cycle above. Recorded here in case this ever
needs setting up again from scratch, on this box or a new one.

1. **`credentials.yaml`** (gitignored) needs `credentials:`/`cookie:` (login)
   plus `census:` and `fred:` API key blocks. Confirmed present on
   wr-mac-studio-1 as of 2026-08-07.
2. **Python deps**: `.venv/bin/pip install -r requirements.txt` — confirmed
   installed (streamlit, folium, streamlit-folium, branca, pdfplumber,
   streamlit-authenticator all present) as of 2026-08-07.
3. **Land Screener's own parcel data** (`output/*.csv`, `data/raw/*.geojson`
   for parcels/zoning/flood/wetlands/buildings/FLU/soil/roads) is gitignored,
   built by running the pipeline once per configured city — NOT shipped with
   the code. Confirmed populated on wr-mac-studio-1 (grand_haven, gh_township,
   spring_lake_twp) as of 2026-08-07. If ever missing on a fresh target:
   ```bash
   cd ~/apps/wm-land-screener/src
   ../.venv/bin/python3 pipeline.py --city grand_haven
   ../.venv/bin/python3 pipeline.py --city gh_township
   ../.venv/bin/python3 pipeline.py --city spring_lake_twp
   ```
   Each city takes a minute or two (downloads live from ~7 external services);
   a failure is most likely a transient network issue with one of them — just
   re-run that city.
4. **The LaunchDaemon plist itself** — if it ever needs installing on a new
   box, it must be copied to `/Library/LaunchDaemons/` (root-owned, requires
   sudo) and loaded with `sudo launchctl load -w
   /Library/LaunchDaemons/co.wakerobin.wm-land-screener.plist`.
