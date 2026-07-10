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

## Steps

### 1. Copy the credentials file up (run from your LOCAL machine)
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

### 4. Verify
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
  line; steps 1, 3, and 4 are standard regardless.
- After go-live, the workflow is: curate/develop locally → merge to `main` →
  push to GitHub → repeat these deploy steps (usually just steps 2–4).
