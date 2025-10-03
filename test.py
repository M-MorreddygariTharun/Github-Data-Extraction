"""
pr_summary_exact_range_with_total.py

- Reads GITHUB_TOKEN from environment (works in GitHub Actions when you add a repo secret).
- Falls back to .env (optional) or interactive prompt for local runs.
- Prompts user for start/end dates
- Only considers PRs CREATED inside the date range
- Counts per developer: Open, Merged, Declined
- Adds Last_Merge_Branch, Latest_Commit_SHA
- Adds Total_PR = Open_PR + Merged_PR + Declined_PR
- Outputs Excel with summary
"""
import os
import time
import requests
import pandas as pd
from datetime import datetime, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Try to load .env if python-dotenv is available (local convenience)
try:
    from dotenv import load_dotenv
    load_dotenv()  # looks for .env in current folder
except Exception:
    pass

# ---------------- CONFIG ----------------
# If you want to hardcode for a quick local test (not recommended), set here:
# GITHUB_TOKEN = "ghp_...."   # <-- don't commit!
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # primary source: environment

REPO_OWNER = "GM-SDV-UP"
REPO_NAME = "gmhmi_fcc"   # repo name
OUT_DIR = r"D:\Tharun kumar reddy\Github-Data-extract-2025-10"
PER_PAGE = 100
# ----------------------------------------

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
        "and the workflow below will inject it into the environment.\n"
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

# ----- GitHub fetch -----
def fetch_all_prs(owner, repo):
    """Fetch all PRs for repo with pagination."""
    meta = session.get(f"{API_BASE}/repos/{owner}/{repo}")
    if meta.status_code != 200:
        raise RuntimeError(f"Cannot access repo {owner}/{repo} (HTTP {meta.status_code}): {meta.text}")

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
    fname = f"{repo}_summary_{start_dt.date()}_to_{end_dt.date()}.xlsx"
    path = os.path.join(out_dir, fname)
    df.to_excel(path, index=True)
    print("Saved:", path)

# ----- Main -----
def main():
    start_dt, end_dt = prompt_date_range()
    print(f"\nFetching PRs for {REPO_OWNER}/{REPO_NAME} ...")
    prs = fetch_all_prs(REPO_OWNER, REPO_NAME)
    print("Fetched:", len(prs), "PRs")
    df = summarize_exact(prs, start_dt, end_dt, REPO_OWNER, REPO_NAME)
    save_df(df, REPO_NAME, start_dt, end_dt, OUT_DIR)

if __name__ == "__main__":
    main()
