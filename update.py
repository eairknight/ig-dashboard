"""
IG Status Dashboard updater.
Reads API_KEY and DASH_PASSWORD from environment variables.
Writes data.json with current account statuses, analytics, and performance metrics.
Run by GitHub Actions daily.

NOTE on reels counting: the Upload-Post history API ignores the user param and returns
global post history for the whole API key. We therefore fetch it once to get the
total live post count, then compute per-account avg as:
  avg_views_per_reel = account_total_views / (total_global_posts / num_active_accounts)
"""

import hashlib
import json
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import pytz
import requests
from requests.adapters import HTTPAdapter

API_BASE = "https://api.upload-post.com"
USERS_URL = f"{API_BASE}/api/uploadposts/users"
MEDIA_URL = f"{API_BASE}/api/uploadposts/media"
ANALYTICS_URL = f"{API_BASE}/api/analytics"
HISTORY_URL = f"{API_BASE}/api/uploadposts/history"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data.json")
URL_MAP_FILE = os.path.join(BASE_DIR, "..", "account_url_map.json")


def make_session(api_key):
    s = requests.Session()
    s.headers["Authorization"] = f"Apikey {api_key}"
    adapter = HTTPAdapter(pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def load_url_map():
    path = os.path.normpath(URL_MAP_FILE)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def classify_status(ig, deep_ok, deep_error):
    if not ig:
        return "NO_IG"
    if ig.get("blocked"):
        return "BLOCKED"
    if ig.get("reauth_required"):
        return "REAUTH"
    if not deep_ok:
        if deep_error and "checkpoint" in deep_error.lower():
            return "CHECKPOINT"
        return "BROKEN"
    return "ACTIVE"


def fetch_global_live_post_count(session, any_username):
    """
    Fetch total live post count across the whole API key.
    The history API ignores the user param and returns global totals, but the param
    is required. We pass any valid username just to satisfy the API requirement.
    """
    try:
        r = session.get(
            HISTORY_URL,
            params={"user": any_username, "platform": "instagram", "page": 1},
            timeout=30,
        )
        if r.status_code == 200:
            return r.json().get("total", 0)
    except Exception:
        pass
    return 0


def fetch_analytics(session, username):
    """Fetch 28-day reach timeseries + aggregate metrics for one account."""
    try:
        r = session.get(
            f"{ANALYTICS_URL}/{username}",
            params={"platforms": "instagram"},
            timeout=30,
        )
        if r.status_code != 200:
            return None
        ig = r.json().get("instagram", {})
        timeseries = ig.get("reach_timeseries", [])
        return {
            "total_views": ig.get("views", 0) or 0,
            "total_reach": ig.get("reach", 0) or 0,
            "total_likes": ig.get("likes", 0) or 0,
            "followers": ig.get("followers", 0) or 0,
            "timeseries": [
                {"date": d["date"], "reach": d["value"]}
                for d in timeseries
            ],
        }
    except Exception:
        return None


def check_account(session, profile, url_map):
    username = profile.get("username", "?")
    ig = profile.get("social_accounts", {}).get("instagram")
    blocked = profile.get("blocked", False)
    assigned_url = url_map.get(username, "")

    base = {
        "username": username,
        "ig_handle": "",
        "display_name": "",
        "profile_pic": "",
        "status": "BLOCKED" if blocked else "NO_IG",
        "reauth_required": False,
        "blocked": blocked,
        "error_msg": None,
        "assigned_url": assigned_url,
        "total_views": 0,
        "total_reach": 0,
        "total_likes": 0,
        "followers": 0,
        "reach_7d": 0,
        "reach_28d": 0,
        "live_reels": 0,
        "avg_views_per_reel": None,
        "avg_reach_per_reel": None,
        "daily_series": [],
    }

    if blocked or not ig:
        return base

    ig_handle = ig.get("handle", "")
    display_name = ig.get("display_name", "")
    profile_pic = ig.get("social_images", "")
    reauth_required = ig.get("reauth_required", False)

    base.update({
        "ig_handle": ig_handle,
        "display_name": display_name,
        "profile_pic": profile_pic,
        "reauth_required": reauth_required,
    })

    if reauth_required:
        base["status"] = "REAUTH"
        base["error_msg"] = "Reconnect required in Upload-Post dashboard"
        return base

    # Deep token check
    deep_ok = False
    deep_error = None
    try:
        r = session.get(MEDIA_URL, params={"platform": "instagram", "user": username}, timeout=30)
        if r.status_code == 200:
            deep_ok = True
        else:
            try:
                deep_error = r.json().get("message", r.text[:120])
            except Exception:
                deep_error = r.text[:120]
    except Exception as e:
        deep_error = str(e)[:120]

    base["status"] = classify_status(ig, deep_ok, deep_error)
    if not deep_ok:
        base["error_msg"] = deep_error

    return base


def enrich_with_analytics(session, account, live_reels_per_account):
    """
    Fetch analytics timeseries and compute per-account stats.
    live_reels_per_account: global total posts / num active accounts.
    """
    username = account["username"]

    analytics = fetch_analytics(session, username)
    if analytics:
        account["total_views"] = int(analytics["total_views"])
        account["total_reach"] = int(analytics["total_reach"])
        account["total_likes"] = int(analytics["total_likes"])
        account["followers"] = int(analytics["followers"])
        timeseries = analytics["timeseries"]
    else:
        timeseries = []

    daily_series = [{"date": d["date"], "reach": d["reach"]} for d in timeseries]
    account["daily_series"] = daily_series

    # 7-day window
    today = datetime.now(timezone.utc).date()
    cutoff_7d = (today - timedelta(days=7)).isoformat()
    recent = [d for d in daily_series if d["date"] >= cutoff_7d]
    account["reach_7d"] = int(sum(d["reach"] for d in recent))
    account["reach_28d"] = int(sum(d["reach"] for d in daily_series))

    # Avg views/reach per reel using global live_reels count (per account)
    account["live_reels"] = live_reels_per_account
    if live_reels_per_account > 0:
        if account["total_views"] > 0:
            account["avg_views_per_reel"] = round(account["total_views"] / live_reels_per_account, 1)
        if account["total_reach"] > 0:
            account["avg_reach_per_reel"] = round(account["total_reach"] / live_reels_per_account, 1)

    return account


def compute_global_series(accounts):
    """Sum daily reach across all accounts to produce a global timeseries."""
    totals = defaultdict(int)
    for acc in accounts:
        for d in acc.get("daily_series", []):
            totals[d["date"]] += d["reach"]

    return [
        {"date": date_str, "reach": totals[date_str]}
        for date_str in sorted(totals.keys())
    ]


def main():
    api_key = os.environ.get("API_KEY", "")
    dash_password = os.environ.get("DASH_PASSWORD", "igdash2026")

    if not api_key:
        print("ERROR: API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    pass_hash = hashlib.sha256(dash_password.encode()).hexdigest()
    url_map = load_url_map()
    session = make_session(api_key)

    print("Fetching profiles...")
    resp = session.get(USERS_URL, timeout=30)
    resp.raise_for_status()
    profiles = resp.json().get("profiles", [])
    print(f"Found {len(profiles)} profiles.")

    # Fetch global live post count (one API call, not per-account)
    # Use any profile username to satisfy the required param
    any_username = profiles[0]["username"] if profiles else "001"
    print("Fetching global live post count...")
    global_post_total = fetch_global_live_post_count(session, any_username)
    print(f"  Total live posts across all accounts: {global_post_total}")

    # Phase 1: status checks (deep token) in parallel
    print("Running deep token checks in parallel...")
    accounts = []
    with ThreadPoolExecutor(max_workers=min(len(profiles), 20)) as pool:
        futures = {pool.submit(check_account, session, p, url_map): p for p in profiles}
        for future in as_completed(futures):
            try:
                accounts.append(future.result())
            except Exception as e:
                print(f"Error checking account: {e}", file=sys.stderr)

    active_accounts = [a for a in accounts if a["status"] == "ACTIVE"]
    inactive_accounts = [a for a in accounts if a["status"] != "ACTIVE"]

    # Compute live reels per account from the global total
    num_active = len(active_accounts) or 1
    live_reels_per_account = round(global_post_total / num_active) if global_post_total > 0 else 0
    print(f"  Live reels per account: ~{live_reels_per_account} ({global_post_total} total / {num_active} active)")

    # Phase 2: analytics enrichment in parallel
    print(f"Fetching analytics for {num_active} active accounts...")
    with ThreadPoolExecutor(max_workers=min(num_active, 15)) as pool:
        futures = {
            pool.submit(enrich_with_analytics, session, a, live_reels_per_account): a
            for a in active_accounts
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error enriching account: {e}", file=sys.stderr)

    all_accounts = active_accounts + inactive_accounts
    all_accounts.sort(key=lambda a: a["username"])

    # Global aggregates
    global_series = compute_global_series(active_accounts)
    today = datetime.now(timezone.utc).date()
    cutoff_7d = (today - timedelta(days=7)).isoformat()
    cutoff_14d = (today - timedelta(days=14)).isoformat()

    recent_global = [d for d in global_series if d["date"] >= cutoff_7d]
    prior_global = [d for d in global_series if cutoff_14d <= d["date"] < cutoff_7d]

    global_reach_7d = int(sum(d["reach"] for d in recent_global))
    global_reach_28d = int(sum(d["reach"] for d in global_series))
    global_total_views = sum(a.get("total_views", 0) for a in active_accounts)
    prior_reach_7d = int(sum(d["reach"] for d in prior_global))
    trend = "up" if global_reach_7d > prior_reach_7d else ("down" if global_reach_7d < prior_reach_7d else "flat")

    global_avg_views_per_reel = (
        round(global_total_views / global_post_total, 1) if global_post_total > 0 else None
    )
    global_avg_reach_per_reel = (
        round(global_reach_28d / global_post_total, 1) if global_post_total > 0 else None
    )

    summary = {s: 0 for s in ("ACTIVE", "REAUTH", "CHECKPOINT", "BROKEN", "NO_IG", "BLOCKED")}
    for a in all_accounts:
        summary[a["status"]] = summary.get(a["status"], 0) + 1

    now_utc = datetime.now(timezone.utc)
    est = pytz.timezone("US/Eastern")
    now_est = now_utc.astimezone(est)
    day = now_est.day
    hour = now_est.hour % 12 or 12
    ampm = "AM" if now_est.hour < 12 else "PM"
    updated_at_display = now_est.strftime(f"%A, %b {day} at {hour}:{now_est.strftime('%M')} {ampm} EST")

    data = {
        "updated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_at_display": updated_at_display,
        "pass_hash": pass_hash,
        "summary": summary,
        "global": {
            "total_views": global_total_views,
            "live_reels_total": global_post_total,
            "live_reels_per_account": live_reels_per_account,
            "reach_7d": global_reach_7d,
            "reach_28d": global_reach_28d,
            "avg_views_per_reel": global_avg_views_per_reel,
            "avg_reach_per_reel": global_avg_reach_per_reel,
            "trend": trend,
            "trend_pct": round((global_reach_7d - prior_reach_7d) / prior_reach_7d * 100, 1) if prior_reach_7d > 0 else None,
            "series": global_series,
        },
        "accounts": all_accounts,
    }

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Wrote {DATA_FILE}")
    print(f"Summary: {summary}")
    print(f"Global 7d reach: {global_reach_7d:,} | avg views/reel: {global_avg_views_per_reel} | trend: {trend}")
    print(f"Live reels: {global_post_total} total / {live_reels_per_account} per account")
    print(f"Updated at: {updated_at_display}")


if __name__ == "__main__":
    main()
