# scripts/dashboard_v2.py
# Youth Soccer Master Index — Mission Control Dashboard (v2)
# ------------------------------------------------------------
# Run: streamlit run scripts/dashboard_v2.py
# or:  python -m streamlit run scripts/dashboard_v2.py

from __future__ import annotations
import os, sys, json, time, subprocess, datetime, logging
from pathlib import Path
import pandas as pd
import streamlit as st
import altair as alt

# ---------- CONFIG ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable
DATA_DIR = REPO_ROOT / "data"
LOG_DIR = DATA_DIR / "logs"
RANKINGS_DIR = DATA_DIR / "rankings"
AUDITS_DIR = DATA_DIR / "audits"
REGISTRY_LOG = LOG_DIR / "registry_history.json"

DEFAULT_STATES = "AZ,CA,CO,FL,GA,ID,LA,MD,MN,VA"
DEFAULT_GENDERS = "M,F"
DEFAULT_AGES = "U10"

st.set_page_config(
    page_title="⚽ Youth Soccer Master Index — Mission Control",
    page_icon="⚙️",
    layout="wide"
)

# ---------- HELPERS ----------
def run_cmd(cmd: list[str], timeout: int = 7200):
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as e:
        return 1, "", f"[ERROR] {e}"

def get_registry_stats() -> dict:
    rc, out, err = run_cmd([PYTHON, "-m", "src.registry.registry", "--stats"])
    if rc != 0:
        return {"_error": err or out}
    try:
        # Use JSONDecoder to find and parse the first valid JSON object
        decoder = json.JSONDecoder()
        # Find the first '{' character
        start_idx = out.find('{')
        if start_idx == -1:
            raise ValueError("No JSON object found in output")
        
        # Parse from the first '{' to the end, handling nested braces
        stats, end_idx = decoder.raw_decode(out[start_idx:])
        stats["timestamp"] = datetime.datetime.now().isoformat()
        save_registry_history(stats)
        return stats
    except json.JSONDecodeError as e:
        return {"_error": f"JSON parse failed: {e}\nOutput: {out[:200]}"}
    except Exception as e:
        return {"_error": f"Failed to extract JSON: {e}\nOutput: {out[:200]}"}

def save_registry_history(entry: dict):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        if REGISTRY_LOG.exists():
            data = json.loads(REGISTRY_LOG.read_text())
        else:
            data = []
        data.append(entry)
        # Keep last 100 records
        data = data[-100:]
        REGISTRY_LOG.write_text(json.dumps(data, indent=2))
    except Exception as e:
        logging.exception("Failed to save registry history: %s", e)

def load_registry_history():
    if REGISTRY_LOG.exists():
        try:
            return pd.DataFrame(json.loads(REGISTRY_LOG.read_text()))
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def latest_audit_csv():
    if not AUDITS_DIR.exists():
        return None
    files = sorted(AUDITS_DIR.glob("identity_audit_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def latest_rank_audit():
    if not RANKINGS_DIR.exists():
        return None
    files = sorted(RANKINGS_DIR.glob("audit_*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def tail_log(logfile: Path, lines: int = 200):
    if not logfile.exists():
        return "[no log yet]"
    with logfile.open("r", encoding="utf-8", errors="ignore") as f:
        return "".join(f.readlines()[-lines:])

# ---------- SIDEBAR MENU ----------
st.sidebar.title("📋 Menu")
menu = st.sidebar.radio(
    "Select an option:",
    [
        "🏠 Overview",
        "📊 Registry Health",
        "🗂️ Slice Builds",
        "🚀 Run Pipeline",
        "🧩 Identity Audit",
        "🕵️ Game Integrity",
        "📈 Charts & Trends",
        "📂 View Audit Files",
        "📢 Slack Controls",
        "🧰 Help & Commands",
    ]
)
st.sidebar.markdown("---")
st.sidebar.caption("Youth Soccer Master Index — Mission Control v2")

# ---------- DASHBOARD PANELS ----------

# 🏠 Overview
if menu == "🏠 Overview":
    st.title("⚽ Youth Soccer Master Index — Mission Control v2")
    st.markdown("""
Welcome to your **central command dashboard** for monitoring, auditing, and running the youth soccer data pipeline.
Use the left menu to:
- View registry health and trends  
- Run scraping & ranking pipelines  
- Audit identity mapping  
- Check for data integrity issues  
- Preview ranking and audit files  
- Trigger Slack notifications  
""")

# 📊 Registry Health
elif menu == "📊 Registry Health":
    st.header("📊 Registry Health Overview")
    stats = get_registry_stats()
    if "_error" in stats:
        st.error(stats["_error"])
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registry Version", stats.get("registry_version"))
        c2.metric("Total Slices", stats.get("total_slices"))
        c3.metric("Total Builds", stats.get("total_builds"))
        c4.metric("Health Score", f"{stats.get('health_score')}%")

        if stats.get("stale_slices"):
            st.warning(f"⚠️ {len(stats['stale_slices'])} stale slices need refresh.")
        else:
            st.success("✅ All slices are up to date")

        df_hist = load_registry_history()
        if not df_hist.empty:
            st.subheader("📈 Health Trend (Last 100 Checks)")
            chart = alt.Chart(df_hist).mark_line(point=True).encode(
                x=alt.X("timestamp:T", title="Timestamp"),
                y=alt.Y("health_score:Q", title="Health Score"),
                color=alt.value("#00bfae")
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)

# 🗂️ Slice Builds
elif menu == "🗂️ Slice Builds":
    st.header("🗂️ Slice Build Registry")
    st.markdown("Real-time view of which build each slice is tracked in.")
    
    from src.registry.registry import get_registry
    registry = get_registry()
    
    # Get all build entries
    builds = registry.list_all_builds()
    
    if not builds:
        st.warning("No slice builds found in registry")
    else:
        # Convert to DataFrame for display
        rows = []
        for slice_key, info in builds.items():
            build_name = info.get('latest_build', 'N/A')
            last_updated = info.get('last_updated', 'N/A')
            
            # Parse timestamp for age calculation
            try:
                from datetime import datetime, timezone
                updated_dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                age_days = (now - updated_dt).days
                
                if age_days == 0:
                    age_str = "Today"
                    status = "✅ Fresh"
                elif age_days <= 7:
                    age_str = f"{age_days} days ago"
                    status = "✅ Recent"
                elif age_days <= 30:
                    age_str = f"{age_days} days ago"
                    status = "⚠️ Aging"
                else:
                    age_str = f"{age_days} days ago"
                    status = "🔴 Stale"
            except:
                age_str = "Unknown"
                status = "❓"
            
            # Parse slice key
            parts = slice_key.split('_')
            if len(parts) >= 3:
                state, gender, age = parts[0], parts[1], parts[2]
            else:
                state, gender, age = slice_key, "", ""
            
            rows.append({
                'Slice': slice_key,
                'State': state,
                'Gender': gender,
                'Age Group': age,
                'Build': build_name,
                'Last Updated': last_updated[:10] if len(last_updated) > 10 else last_updated,
                'Age': age_str,
                'Status': status
            })
        
        df = pd.DataFrame(rows)
        
        # Add filters
        col1, col2, col3 = st.columns(3)
        with col1:
            state_filter = st.multiselect("Filter by State", sorted(df['State'].unique()))
        with col2:
            gender_filter = st.multiselect("Filter by Gender", sorted(df['Gender'].unique()))
        with col3:
            status_filter = st.multiselect("Filter by Status", sorted(df['Status'].unique()))
        
        # Apply filters
        filtered = df
        if state_filter:
            filtered = filtered[filtered['State'].isin(state_filter)]
        if gender_filter:
            filtered = filtered[filtered['Gender'].isin(gender_filter)]
        if status_filter:
            filtered = filtered[filtered['Status'].isin(status_filter)]
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Slices", len(filtered))
        with col2:
            fresh_count = len(filtered[filtered['Status'].str.contains('Fresh|Recent')])
            st.metric("Fresh/Recent", fresh_count)
        with col3:
            stale_count = len(filtered[filtered['Status'].str.contains('Stale')])
            st.metric("Stale", stale_count)
        with col4:
            unique_builds = filtered['Build'].nunique()
            st.metric("Unique Builds", unique_builds)
        
        # Display table
        st.dataframe(
            filtered,
            use_container_width=True,
            hide_index=True
        )
        
        # Show registry version
        version = registry.get_registry_version()
        st.caption(f"Registry Version: {version}")

# 🚀 Run Pipeline
elif menu == "🚀 Run Pipeline":
    st.header("🚀 Run Full Pipeline")
    states = st.text_input("States", DEFAULT_STATES)
    genders = st.text_input("Genders", DEFAULT_GENDERS)
    ages = st.text_input("Ages", DEFAULT_AGES)
    dry_run = st.checkbox("Dry run only", False)
    refresh = st.checkbox("Refresh normalized data", False)
    tuner = st.checkbox("Run tuner after ranking", False)
    if st.button("Run Pipeline"):
        with st.spinner("Running pipeline... check Slack for notifications"):
            cmd = [PYTHON, "scripts/pipeline_runner.py", "--states", states, "--genders", genders, "--ages", ages]
            if dry_run:
                cmd.append("--dry-run")
            if refresh:
                cmd.append("--refresh-normalized")
            if tuner:
                cmd.append("--with-tuner")
            rc, out, err = run_cmd(cmd, timeout=8*3600)
            if rc == 0:
                st.success("✅ Pipeline completed successfully.")
            else:
                st.error(f"❌ Pipeline failed (exit {rc})")
            with st.expander("Show Logs"):
                st.code(out or err)

# 🧩 Identity Audit
elif menu == "🧩 Identity Audit":
    st.header("🧩 Identity Audit & Duplicates")
    threshold = st.slider("Similarity Threshold", 60, 95, 85)
    export = st.checkbox("Export CSV Report")
    weekly = st.checkbox("Send Weekly Summary to Slack")
    if st.button("Run Identity Audit"):
        with st.spinner("Running identity audit..."):
            cmd = [PYTHON, "-m", "src.identity.identity_audit", "--threshold", str(threshold)]
            if export:
                AUDITS_DIR.mkdir(parents=True, exist_ok=True)
                path = AUDITS_DIR / f"identity_audit_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                cmd += ["--export", str(path)]
            if weekly:
                cmd.append("--weekly-summary")
            rc, out, err = run_cmd(cmd)
            if rc == 0:
                st.success("✅ Audit completed.")
            else:
                st.error(f"❌ Audit failed (exit {rc})")
            st.code(out or err)

    latest = latest_audit_csv()
    if latest:
        st.markdown(f"### 📄 Latest Audit: `{latest.name}`")
        df = pd.read_csv(latest)
        st.dataframe(df.head(50))
        st.download_button("Download Full Audit CSV", latest.read_bytes(), file_name=latest.name)

# 🕵️ Game Integrity
elif menu == "🕵️ Game Integrity":
    st.header("🕵️ Game Integrity Checker (Hash Verification)")
    check_all = st.button("🔍 Check All Hashes")
    refresh = st.button("♻️ Rebuild All Hashes")
    if check_all:
        rc, out, err = run_cmd([PYTHON, "-m", "src.scraper.utils.game_hash_checker", "--check-all"])
        st.code(out or err)
        if rc == 0:
            st.success("✅ Integrity check completed.")
        else:
            st.error("❌ Integrity check failed.")
    if refresh:
        rc, out, err = run_cmd([PYTHON, "-m", "src.scraper.utils.game_hash_checker", "--refresh"])
        st.code(out or err)

# 📈 Charts & Trends
elif menu == "📈 Charts & Trends":
    st.header("📈 System Trends & Visuals")
    df_hist = load_registry_history()
    if df_hist.empty:
        st.info("No registry history yet. Run health checks first.")
    else:
        c1, c2 = st.columns(2)
        chart1 = alt.Chart(df_hist).mark_line(point=True).encode(
            x="timestamp:T", y="health_score:Q", color=alt.value("#1f77b4")
        ).properties(title="Registry Health Over Time")
        c1.altair_chart(chart1, use_container_width=True)

        if "total_slices" in df_hist.columns:
            chart2 = alt.Chart(df_hist).mark_bar().encode(
                x="timestamp:T", y="total_slices:Q", color=alt.value("#ff7f0e")
            ).properties(title="Total Slices Over Time")
            c2.altair_chart(chart2, use_container_width=True)

# 📂 View Audit Files
elif menu == "📂 View Audit Files":
    st.header("📂 Recent Audit & Ranking Files")
    rank_audit = latest_rank_audit()
    identity_audit = latest_audit_csv()
    if identity_audit:
        st.subheader(f"🧩 Identity Audit: {identity_audit.name}")
        st.dataframe(pd.read_csv(identity_audit).head(50))
        st.download_button("Download", identity_audit.read_bytes(), file_name=identity_audit.name)
    if rank_audit:
        st.subheader(f"📊 Ranking Audit: {rank_audit.name}")
        df = pd.read_parquet(rank_audit)
        st.dataframe(df.head(50))
        st.download_button("Download", rank_audit.read_bytes(), file_name=rank_audit.name)

# 📢 Slack Controls
elif menu == "📢 Slack Controls":
    st.header("📢 Slack Notification Controls")
    st.markdown("Test or send manual Slack notifications below.")
    if st.button("Send Registry Health Now"):
        cmd = [PYTHON, "-m", "src.utils.notifier", "--message", "📊 Manual Registry Health Check Triggered"]
        rc, out, err = run_cmd(cmd)
        st.code(out or err)
    if st.button("Test Slack Connection"):
        cmd = [PYTHON, "-m", "src.utils.notifier", "--test"]
        rc, out, err = run_cmd(cmd)
        st.code(out or err)
        if rc == 0:
            st.success("✅ Slack connected.")
        else:
            st.error("❌ Slack test failed.")

# 🧰 Help & Commands
elif menu == "🧰 Help & Commands":
    st.header("🧰 Command Reference Menu")
    st.markdown("""
### Core Commands
- `python scripts/pipeline_runner.py --states AZ,NV --genders M,F --ages U10`
- `python scripts/pipeline_runner.py --refresh-normalized`
- `python scripts/pipeline_runner.py --with-tuner`

### Registry
- `python -m src.registry.registry --stats`
- `python -m src.registry.registry --check-version`

### Identity & Integrity
- `python -m src.identity.identity_audit --threshold 85 --export data/audits/audit.csv`
- `python -m src.scraper.utils.game_hash_checker --check-all`

### Slack
- `python -m src.utils.notifier --test`
- `python -m src.utils.notifier --message "Manual Alert"`

Logs are in `data/logs/pipeline_runner.log` and `pipeline_errors.log`.
""")
    st.info("All commands can also be run from within this dashboard.")
