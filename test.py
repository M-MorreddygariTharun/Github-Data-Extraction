"""
pr_summary_exact_range_with_total.py  (saved as test.py)

- Reads GITHUB_TOKEN from environment (works in GitHub Actions when you add a repo secret).
- Accepts START_DATE and END_DATE via environment (format: YYYY-MM-DD or any parseable format).
  If not provided and running interactively, will prompt the user.
  If not provided and not interactive (CI), will default to the last 24 hours.
- Accepts OUT_DIR via environment (defaults to original OUT_DIR variable if not set).
- Auto-detects REPO_OWNER/REPO_NAME from GITHUB_REPOSITORY if available.
- Produces an Excel artifact and prints its path (OUTPUT_FILE: <path>).
"""
import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Try to load .env if python-dotenv is available (local convenience)
try:
    from dotenv import load_dotenv
    load_dotenv()  # looks for .env in current folder
except Exception:
    pass

# ---------------- CONFIG (improved, auto-detect in Actions) ----------------
# GITHUB_TOKEN uses environment first
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # primary source: environment

# If running inside GitHub Actions, GITHUB_REPOSITORY is provided (owner/repo)
_github_repo_env = os.getenv("GITHUB_REPOSITORY", "").strip()
if _github_repo_env and "/" in _github_repo_env:
    REPO_OWNER, REPO_NAME = _github_repo_env.split("/", 1)
else:
    # fall back to environment variables or hardcoded defaults
    REPO_OWNER = os.getenv("REPO_OWNER", "GM-SDV-UP")
    REPO_NAME = os.getenv("REPO_NAME", "gmhmi_fcc")

# Out dir: prefer environment (Actions will set to workspace/artifacts)
DEFAULT_OUT_DIR = r"D:\Tharun kumar reddy\Github-Data-extract-2025-10"
OUT_DIR = os.getenv("OUT_DIR", DEFAULT_OUT_DIR)

# PER_PAGE can be overridden via env
PER_PAGE = int(os.getenv("PER_PAGE", 100))
# ---------------------------------------------------------------------------

if not GITHUB_TOKEN or GITHUB_TOKEN.strip() == "":
    # Friendly failure: if running locally, instruct how to provide a token
    msg = (
        "ERROR: GITHUB_TOKEN not found in environment.\n\n"
        "If you are running this script locally, set the environment variable first:\n\n"
        "  Windows (PowerShell):\n"
        "    $env:GITHUB_TOKEN = 'ghp_xxx'\n\n"
        "  Windows (persist):\n"
        "    setx GITHUB_TOKEN \"ghp_xxx\"\n\n"
        "  Linux / macOS:\n"
        "    export GITHUB_TOKEN=ghp_xxx\n\n"
        "Or create a .env file next to this script with:\n"
        "  GITHUB_TOKEN=ghp_xxx\n\n"
        "If you intend to run from GitHub Actions, add your token as a repository secret\n"
        "and the workflow will inject it into the environment.\n"
    )
    raise SystemExit(msg)

API_BASE = "https://api.github.com"
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1,
                                                       status_forcelist=[429,500,502,503,504])))
session.headers.update({"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"})

# ----- Utility -----
def parse_user_date(s: str):
    """Parse many date formats. If year missing, assume current year."""
    s = s.strip()
    today_year = date.today().year
    fmts = ["%Y-%m-%d", "%b %d %Y", "%b %d", "%d %b %Y", "%d %b",
            "%d-%b-%Y", "%d %B %Y", "%B %d %Y", "%B %d"]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            if "%Y" not in f:
                dt = dt.replace(year=today_year)
            return dt
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s)
    except Exception:
        raise ValueError("Could not parse date. Use formats like 2025-09-01 or Sep 1 2025.")

def prompt_date_range():
    print("Enter start and end dates (only PRs created in this range are considered).")
    s = input("Start date: ").strip()
    e = input("End date:   ").strip()
    start = parse_user_date(s).replace(hour=0, minute=0, second=0, microsecond=0)
    end = parse_user_date(e).replace(hour=23, minute=59, second=59, microsecond=999999)
    if end < start:
        raise SystemExit("End date before start date.")
    return start, end

def parse_iso_datetime(iso):
    if not iso:
        return None
    try:
        return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        try:
            return datetime.fromisoformat(iso.replace("Z", "+00:00"))
        except Exception:
            try:
                return datetime.strptime(str(iso).split("T")[0], "%Y-%m-%d")
            except Exception:
                return None

# ----- GitHub fetch (verbose/diagnostic) -----
def fetch_all_prs(owner, repo):
    """Fetch all PRs for repo with pagination. Raises verbose errors on failure."""
    repo_url = f"{API_BASE}/repos/{owner}/{repo}"
    print(f"DEBUG: attempting to access repository via API: {repo_url}")
    meta = session.get(repo_url)
    if meta.status_code != 200:
        # Print body (safe for logs) and helpful hint
        try:
            body = meta.json()
        except Exception:
            body = meta.text
        raise RuntimeError(
            f"Cannot access repo {owner}/{repo} (HTTP {meta.status_code}): {body}\n\n"
            "Possible causes:\n"
            " - The repo name or owner is incorrect (check case-sensitive spelling).\n"
            " - The token lacks access (private repo requires a token with 'repo' scope).\n"
            " - Running from a forked PR where secrets are not available.\n\n"
            "Actionable checks:\n"
            f" - Confirm {repo_url} exists in the browser.\n"
            " - In Actions, ensure you used secrets.GITHUB_TOKEN or a PAT with repo permissions.\n"
            " - Print environment vars in the job to confirm REPO_OWNER/REPO_NAME/GITHUB_REPOSITORY.\n"
        )

    all_prs, page = [], 1
    while True:
        r = session.get(f"{API_BASE}/repos/{owner}/{repo}/pulls",
                        params={"state": "all", "per_page": PER_PAGE, "page": page})
        r.raise_for_status()
        items = r.json()
        if not items:
            break
        all_prs.extend(items)
        page += 1
        time.sleep(0.12)
    return all_prs

# ----- Summarize -----
def summarize_exact(prs, start_dt, end_dt, owner, repo):
    """
    Only PRs with created_at inside [start_dt, end_dt] are considered.
    For each such PR:
      - merged_at in range => Merged
      - closed_at in range => Declined
      - else => Open
    """
    created_in_range = []
    for pr in prs:
        created_dt = parse_iso_datetime(pr.get("created_at"))
        if created_dt and start_dt <= created_dt <= end_dt:
            created_in_range.append((pr, created_dt))

    by_dev = {}
    for pr, created_dt in created_in_range:
        login = pr.get("user", {}).get("login", "unknown_user")
        by_dev.setdefault(login, []).append((pr, created_dt))

    rows = []
    for dev, items in by_dev.items():
        open_count = merged_count = declined_count = 0
        earliest_created = None
        latest_merged_dt = None
        latest_merged_branch = None
        latest_merged_commit = None

        for pr, created_dt in items:
            if earliest_created is None or created_dt < earliest_created:
                earliest_created = created_dt

            merged_dt = parse_iso_datetime(pr.get("merged_at"))
            closed_dt = parse_iso_datetime(pr.get("closed_at"))

            merged_in_range = merged_dt and start_dt <= merged_dt <= end_dt
            closed_in_range = closed_dt and start_dt <= closed_dt <= end_dt

            if merged_in_range:
                merged_count += 1
                if latest_merged_dt is None or merged_dt > latest_merged_dt:
                    latest_merged_dt = merged_dt
                    latest_merged_branch = pr.get("base", {}).get("ref")
                    latest_merged_commit = pr.get("head", {}).get("sha")
            elif closed_in_range:
                declined_count += 1
            else:
                open_count += 1

        total_pr = open_count + merged_count + declined_count

        rows.append({
            "Developer_Email_ID": f"{dev}@github",
            "Repo": f"{owner}/{repo}",
            "Last_Merge_Branch": latest_merged_branch or "None",
            "Open_PR": open_count,
            "Merged_PR": merged_count,
            "Declined_PR": declined_count,
            "Total_PR": total_pr,
            "Open_PR_DateTime": earliest_created.strftime("%Y-%m-%d %H:%M:%S") if earliest_created else "NA",
            "Close_PR_DateTime": latest_merged_dt.strftime("%Y-%m-%d %H:%M:%S") if latest_merged_dt else "NA",
            "Declined_PR_DateTime": "NA",
            "Ages_of_Open_PR": "NA",
            "Ages_of_Close_PR": "NA",
            "Latest_Commit_SHA": latest_merged_commit or "NA"
        })

    cols = ["Developer_Email_ID","Repo","Last_Merge_Branch","Open_PR","Merged_PR","Declined_PR","Total_PR",
            "Open_PR_DateTime","Close_PR_DateTime","Declined_PR_DateTime","Ages_of_Open_PR","Ages_of_Close_PR","Latest_Commit_SHA"]

    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows, columns=cols)

# ----- Save -----
def save_df(df, repo, start_dt, end_dt, out_dir):
    df = df.copy()
    df.index = range(1, len(df) + 1)
    df.index.name = "Index"
    # sanitize dates for filename
    s = start_dt.date().isoformat()
    e = end_dt.date().isoformat()
    fname = f"{repo}_summary_{s}_to_{e}.xlsx"
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, fname)
    df.to_excel(path, index=True)
    print("Saved:", path)
    return path

# ----- Main -----
def main():
    # Determine start/end dates
    env_start = os.getenv("START_DATE")
    env_end = os.getenv("END_DATE")

    # If env provided, use them
    if env_start and env_end:
        start_dt = parse_user_date(env_start).replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = parse_user_date(env_end).replace(hour=23, minute=59, second=59, microsecond=999999)
    else:
        # If interactive, prompt user (local)
        if sys.stdin and sys.stdin.isatty():
            start_dt, end_dt = prompt_date_range()
        else:
            # Non-interactive default: last 24 hours
            now = datetime.utcnow()
            yesterday = now - timedelta(days=1)
            start_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            end_dt = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            print(f"No START_DATE/END_DATE provided and not interactive. Defaulting to last 24hrs: {start_dt} -> {end_dt}")

    print(f"\nFetching PRs for {REPO_OWNER}/{REPO_NAME} ...")
    prs = fetch_all_prs(REPO_OWNER, REPO_NAME)
    print("Fetched:", len(prs), "PRs")
    df = summarize_exact(prs, start_dt, end_dt, REPO_OWNER, REPO_NAME)
    saved_path = save_df(df, REPO_NAME, start_dt, end_dt, OUT_DIR)
    # print path for CI logs
    print("OUTPUT_FILE:", saved_path)

if __name__ == "__main__":
    main()
