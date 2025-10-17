# scripts/dashboard.py
# Streamlit Control Center for Youth Soccer Master Index
# ------------------------------------------------------
# Runs locally:  streamlit run scripts/dashboard.py
# Requires: streamlit, pandas, pyarrow (for parquet), python-dotenv (optional)

from __future__ import annotations
import os, sys, json, time, subprocess, textwrap, datetime
from pathlib import Path
import streamlit as st

# ---------- CONFIG ----------
REPO_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
PYTHON = sys.executable

# Common defaults (edit to taste)
DEFAULT_STATES  = "AZ,CA,CO,FL,GA,ID,LA,MD,MN,VA"
DEFAULT_GENDERS = "M,F"
DEFAULT_AGES    = "U10"

LOG_DIR = REPO_ROOT / "data" / "logs"
RANKINGS_DIR = REPO_ROOT / "data" / "rankings"
AUDITS_DIR = REPO_ROOT / "data" / "audits"

# ---------- HELPERS ----------
def run_cmd(cmd: list[str], env: dict | None = None, cwd: Path | None = None, timeout: int = 3600):
    """Run a subprocess, capture output, return (code, stdout, stderr)."""
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd or REPO_ROOT),
            env={**os.environ, **(env or {})},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as e:
        return 124, e.stdout or "", f"[TIMEOUT] {e}"
    except Exception as e:
        return 1, "", f"[ERROR] {e}"

def code_block(s: str) -> str:
    return f"```\n{textwrap.dedent(s).strip()}\n```"

@st.cache_data(show_spinner=False)
def get_registry_stats_cached() -> dict:
    # python -m src.registry.registry --stats  (expects JSON on stdout)
    rc, out, err = run_cmd([PYTHON, "-m", "src.registry.registry", "--stats"])
    if rc != 0:
        return {"_error": err.strip() or out.strip() or f"non-zero exit {rc}"}
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        # attempt to strip possible logs above JSON
        try:
            js = out[out.find("{") : out.rfind("}") + 1]
            return json.loads(js)
        except Exception:
            return {"_error": f"failed to parse JSON: {out[:400]}"}

def trigger_pipeline(states: str, genders: str, ages: str, dry_run: bool, refresh_normalized: bool, with_tuner: bool):
    cmd = [PYTHON, "scripts/pipeline_runner.py", "--states", states, "--genders", genders, "--ages", ages]
    if dry_run:
        cmd.append("--dry-run")
    if refresh_normalized:
        cmd.append("--refresh-normalized")
    if with_tuner:
        cmd.append("--with-tuner")
    return run_cmd(cmd, timeout=24*3600)  # up to 24h for big runs

def run_identity_audit(threshold: int, export_csv: bool, weekly_summary: bool):
    cmd = [PYTHON, "-m", "src.identity.identity_audit", "--threshold", str(threshold)]
    if export_csv:
        AUDITS_DIR.mkdir(parents=True, exist_ok=True)
        export_path = AUDITS_DIR / f"identity_audit_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        cmd += ["--export", str(export_path)]
    if weekly_summary:
        cmd.append("--weekly-summary")
    return run_cmd(cmd)

def run_hash_check(refresh: bool, check_all: bool):
    cmd = [PYTHON, "-m", "src.scraper.utils.game_hash_checker"]
    if refresh:
        cmd.append("--refresh")
    if check_all:
        cmd.append("--check-all")
    return run_cmd(cmd, timeout=3*3600)

def latest_rank_audits(n: int = 10):
    if not RANKINGS_DIR.exists():
        return []
    files = sorted(RANKINGS_DIR.glob("audit_*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[:n]

def tail_file(path: Path, lines: int = 200) -> str:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            content = f.readlines()
        return "".join(content[-lines:])
    except Exception as e:
        return f"[could not read log] {e}"

# ---------- UI ----------
st.set_page_config(page_title="Youth Soccer Master Index — Control Center", page_icon="⚽", layout="wide")
st.title("⚽ Youth Soccer Master Index — Control Center")

# Tabs: Overview | Registry | Pipeline | Integrity | Identity | Rankings | Logs | Help
tabs = st.tabs(["Overview", "Registry", "Pipeline", "Integrity", "Identity", "Rankings", "Logs", "Help"])

# --- Overview ---
with tabs[0]:
    st.subheader("System Health at a Glance")
    stats = get_registry_stats_cached()
    if "_error" in stats:
        st.error("Registry stats error")
        st.code(stats["_error"])
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registry Version", stats.get("registry_version", "—"))
        c2.metric("Total Slices", stats.get("total_slices", 0))
        c3.metric("Total Builds", stats.get("total_builds", 0))
        c4.metric("Health Score", f"{stats.get('health_score', 0)}%")

        stale = stats.get("stale_slices", [])
        if stale:
            st.warning(f"⚠️ {len(stale)} stale slices (>7 days). Example: {', '.join(stale[:5])}{'…' if len(stale) > 5 else ''}")
        else:
            st.success("✅ All slices are fresh within 7 days")

        st.caption("Tip: use the Pipeline tab to run multi-slice refreshes; Slack will notify on start/finish.")

# --- Registry ---
with tabs[1]:
    st.subheader("Registry: Stats & Version")
    colL, colR = st.columns([2, 1])
    with colL:
        rc, out, err = run_cmd([PYTHON, "-m", "src.registry.registry", "--stats"])
        st.markdown("**Command:**")
        st.code("python -m src.registry.registry --stats")
        if rc == 0:
            st.json(json.loads(out[out.find("{"): out.rfind("}") + 1]))
        else:
            st.error("Failed to fetch stats")
            st.code(err or out)
    with colR:
        st.markdown("**Version Check**")
        rc2, out2, err2 = run_cmd([PYTHON, "-m", "src.registry.registry", "--check-version"])
        st.code("python -m src.registry.registry --check-version")
        st.text(out2 if rc2 == 0 else (err2 or out2))

# --- Pipeline ---
with tabs[2]:
    st.subheader("Run Pipeline")
    with st.form("pipeline_form"):
        states  = st.text_input("States (CSV)", value=DEFAULT_STATES)
        genders = st.text_input("Genders (CSV)", value=DEFAULT_GENDERS)
        ages    = st.text_input("Ages (CSV)", value=DEFAULT_AGES)
        dry_run = st.checkbox("Dry run (no writes)", value=False)
        refresh = st.checkbox("Refresh normalized (rebuild from all builds)", value=False)
        with_tuner = st.checkbox("Run tuner after rankings", value=False)

        submitted = st.form_submit_button("🚀 Run Pipeline")
        if submitted:
            st.info("Starting pipeline… watch Slack for notifications.")
            rc, stdout, stderr = trigger_pipeline(states, genders, ages, dry_run, refresh, with_tuner)
            if rc == 0:
                st.success("Pipeline finished.")
            else:
                st.error(f"Pipeline failed (exit {rc}).")
            with st.expander("stdout"):
                st.code(stdout or "[no stdout]")
            with st.expander("stderr"):
                st.code(stderr or "[no stderr]")

# --- Integrity ---
with tabs[3]:
    st.subheader("Game Integrity (Hash Checking)")
    colA, colB = st.columns(2)
    with colA:
        st.markdown("**Check all slices**")
        if st.button("🔍 Check Hashes (All Slices)"):
            rc, out, err = run_hash_check(refresh=False, check_all=True)
            if rc == 0:
                st.success("Hash check done.")
            else:
                st.error(f"Hash check failed (exit {rc}).")
            st.code(out or err or "[no output]")
    with colB:
        st.markdown("**Rebuild all hashes**")
        if st.button("♻️ Recompute Hashes"):
            rc, out, err = run_hash_check(refresh=True, check_all=False)
            if rc == 0:
                st.success("Hash library refreshed.")
            else:
                st.error(f"Refresh failed (exit {rc}).")
            st.code(out or err or "[no output]")

# --- Identity ---
with tabs[4]:
    st.subheader("Identity Audit")
    th = st.slider("Similarity Threshold", min_value=60, max_value=95, value=85, step=1)
    export = st.checkbox("Export CSV report")
    weekly = st.checkbox("Send weekly summary (Slack)")
    if st.button("🧩 Run Identity Audit"):
        rc, out, err = run_identity_audit(threshold=th, export_csv=export, weekly_summary=weekly)
        if rc == 0:
            st.success("Audit run complete.")
        else:
            st.error(f"Audit failed (exit {rc}).")
        st.code(out or err or "[no output]")

# --- Rankings ---
with tabs[5]:
    st.subheader("Recent Ranking Audit Files")
    files = latest_rank_audits(20)
    if not files:
        st.info("No audit parquet files found yet.")
    else:
        for p in files:
            st.write(f"• `{p.name}`  —  modified {datetime.datetime.fromtimestamp(p.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption("These files are generated automatically by ranking_engine and contain per-team transparency metrics.")

# --- Logs ---
with tabs[6]:
    st.subheader("Recent Logs")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    runner_log = LOG_DIR / "pipeline_runner.log"
    errors_log = LOG_DIR / "pipeline_errors.log"

    colL, colR = st.columns(2)
    with colL:
        st.markdown("**pipeline_runner.log (tail)**")
        st.code(tail_file(runner_log, lines=400))
    with colR:
        st.markdown("**pipeline_errors.log (tail)**")
        st.code(tail_file(errors_log, lines=400))

# --- Help ---
with tabs[7]:
    st.subheader("Quick Commands")
    st.markdown("""
- **Run orchestrator (multi-slice)**  
  `python scripts/pipeline_runner.py --states AZ,NV --genders M,F --ages U10,U11`

- **Registry stats / version**  
  `python -m src.registry.registry --stats`  
  `python -m src.registry.registry --check-version`

- **Identity audit**  
  `python -m src.identity.identity_audit --threshold 85 --export data/audits/identity_audit_latest.csv`

- **Hash checker**  
  `python -m src.scraper.utils.game_hash_checker --check-all`
    """)
    st.caption("All pipeline events post to Slack if SLACK_WEBHOOK_URL is set.")
