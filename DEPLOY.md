# Deploying the new unified app to production

The app was restructured from a standalone Land Screener into a **navigation
shell** (home page → Market Feasibility → Land Screener → Financial Review),
behind a login. Production currently still runs the OLD standalone version.
These are the one-time changes to bring the live site up to the current `main`.

**Requires SSH access to the EC2 box (`ubuntu@44.213.16.32`).**

## What changed (why these steps exist)
1. **Entry point moved.** The app now starts from `src/app_shell.py`, not
   `src/app.py` (which is now an internal module). The systemd service in the
   repo has been updated to launch `app_shell.py`; it must be re-copied + the
   service reloaded.
2. **Login + Census key file is not in git.** `credentials.yaml` is gitignored
   (it holds the login usernames/passwords **and** the Census API key the market
   data reads). The server needs its own copy — easiest is to copy the working
   local one up.
3. **New libraries.** Maps, login, PDF parsing, etc. add dependencies
   (`streamlit-authenticator`, `folium`, `streamlit-folium`, `branca`,
   `pdfplumber`, …). `requirements.txt` must be re-installed.

The economic-development data (`data/econ_dev_queue.json`) IS tracked in git now,
so it deploys with the code — no separate copy needed.

4. **Econ-dev scanning/curation is local-only.** The systemd service now sets
   `WR_DEPLOY_ENV=production`, which hides the Scan now / review inbox / editable
   kept-items tables in the deployed app — everyone using the live site sees only
   the read-only Executive pins + summary. This is deliberate: it stops the
   live server's own copy of `econ_dev_queue.json` from being edited directly (and
   drifting out of sync with git) once more than one person is using the site.
   Curation always happens locally, then commit → push → redeploy as usual.
5. **Land Screener's own parcel data is never in git and won't exist on a new
   server.** `output/*.csv` and `data/raw/*.geojson` (parcels, zoning, flood,
   wetlands, buildings, FLU, soil, roads — one set per configured city) are all
   gitignored cache, built by running the pipeline, not shipped with the code.
   A freshly deployed server has none of this — the Land Screener page will show
   "No data found... Click ▶ Run" or a "Pipeline failed" error (confirmed
   2026-08-03 on the wr-mac-studio-1 dev box: `output/` was completely empty
   because the pipeline had simply never been run there). This is a one-time
   step per server/city, not something that needs repeating — once built, the
   cache persists on disk and only needs a manual "Refresh" if you want newer
   source data later.

## Steps

### 1. Make sure `credentials.yaml` on the server has the logins AND the Census key
The server's `credentials.yaml` (gitignored, never in the repo) must contain the
existing login block **and** a new `census:` block — the market-feasibility
features read the Census API key from here. The prior setup predates this key, so
it likely needs adding:

```yaml
census:
  api_key: <ASK SADIE FOR THE KEY — not stored in git>
credentials:
  usernames:
    ...        # existing logins (unchanged)
cookie:
  ...          # existing cookie block (unchanged)
```

Either add the `census:` block to the server's existing file, **or** copy the
current working local file up (it already has logins + key):
```bash
scp credentials.yaml ubuntu@44.213.16.32:/home/ubuntu/wm-land-screener/credentials.yaml
```

### 2. SSH in and update the code
```bash
ssh ubuntu@44.213.16.32
cd /home/ubuntu/wm-land-screener

# ⚠️ CONFIRM how this box takes updates. If the working dir tracks GitHub:
git fetch origin
git checkout main          # the working dir may currently be on another branch
git pull origin main
# (If a `git push live` post-receive hook is used instead, deploy that way and
#  skip this block — but still do steps 3–4 below.)
```

### 3. Install dependencies + update the service
```bash
./.venv/bin/pip install -r requirements.txt
sudo cp deploy/wm-land-screener.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart wm-land-screener
```

### 4. Populate Land Screener's parcel data (first deploy only)
Run the pipeline once per configured city (`config.py: CITIES` — currently
`grand_haven`, `gh_township`, `spring_lake_twp`) so `output/` and
`data/raw/*.geojson` exist. Skip this step on a repeat deploy to an already-set-up
server — it's one-time per server, not part of the regular code-update cycle.
```bash
cd /home/ubuntu/wm-land-screener/src
../.venv/bin/python3 pipeline.py --city grand_haven
../.venv/bin/python3 pipeline.py --city gh_township
../.venv/bin/python3 pipeline.py --city spring_lake_twp
```
Each city takes a minute or two (downloads parcels/zoning/flood/wetlands/buildings/
FLU/soil/roads live from ~7 external services) and prints a summary of parcels
loaded / passed filters when done. If one fails, it's most likely a transient
network issue with one of those services — just re-run that city.

### 5. Verify
```bash
systemctl status wm-land-screener --no-pager      # should be active (running)
journalctl -u wm-land-screener -n 40 --no-pager   # check for startup errors
```
Then open the site — you should get the **login page**, then the full three-section app.

## Rollback (if something's wrong)
```bash
cd /home/ubuntu/wm-land-screener
git checkout <previous-commit>       # e.g. the commit that was live before
sudo systemctl restart wm-land-screener
```

## Notes
- The only server-specific unknown is **step 2** (how the box pulls code / whether
  there's an auto-deploy hook). Whoever set up the server should confirm/adapt that
  line; steps 1, 3, and 5 are standard regardless.
- Step 4 (populate parcel data) is **one-time per server**, not part of the regular
  update cycle — skip it once a server already has its `output/`/`data/raw/`
  parcel caches, same as step 1 (credentials) usually only matters on first setup.
- After go-live, the workflow is: curate/develop locally → merge to `main` →
  push to GitHub → repeat these deploy steps (usually just steps 2, 3, and 5).
