"""
IG Status Dashboard updater.
Reads API_KEY and DASH_PASSWORD from environment variables.
Writes data.json with current account statuses, analytics, and performance metrics.
Run by GitHub Actions daily.
"""

import hashlib
import json
import os
import re
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
# account_url_map.json lives one level up (project root)
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
            "total_comments": ig.get("comments", 0) or 0,
            "followers": ig.get("followers", 0) or 0,
            "timeseries": [
                {"date": d["date"], "reach": d["value"]}
                for d in timeseries
            ],
        }
    except Exception:
        return None


def fetch_history_posts(session, username, pages=3):
    """Fetch recent post history (up to pages*50 posts) and return list of {date, url}."""
    posts = []
    for page in range(1, pages + 1):
        try:
            r = session.get(
                HISTORY_URL,
                params={"user": username, "platform": "instagram", "page": page, "limit": 50},
                timeout=30,
            )
            if r.status_code != 200:
                break
            data = r.json()
            batch = data.get("history", [])
            if not batch:
                break
            for post in batch:
                ts = (post.get("upload_timestamp") or "")[:10]
                caption = post.get("post_title") or post.get("post_caption") or ""
                found = re.findall(r"([A-Za-z0-9-]+\.com)", caption, re.IGNORECASE)
                url = found[0] if found else None
                if ts:
                    posts.append({"date": ts, "url": url})
            # Stop if we've collected enough (covers ~15 days at 10/day)
            if len(posts) >= 150:
                break
        except Exception:
            break
    return posts


def compute_reels_per_day(history_posts):
    """Returns {date_str: count} from history."""
    counts = defaultdict(int)
    for p in history_posts:
        if p["date"]:
            counts[p["date"]] += 1
    return dict(counts)


def compute_avg_reach_per_reel(timeseries, reels_per_day):
    """
    For each day in timeseries, compute avg_reach_per_reel = daily_reach / reels_posted.
    Returns list of {date, reach, reels, avg} — None avg when reels=0.
    """
    result = []
    for entry in timeseries:
        d = entry["date"]
        reach = entry["reach"]
        reels = reels_per_day.get(d, 0)
        avg = round(reach / reels, 1) if reels > 0 and reach > 0 else None
        result.append({"date": d, "reach": reach, "reels": reels, "avg": avg})
    return result


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
        # Analytics fields (filled in separately)
        "total_views": 0,
        "total_reach": 0,
        "total_likes": 0,
        "followers": 0,
        "reach_7d": 0,
        "reach_28d": 0,
        "reels_7d": 0,
        "avg_reach_per_reel_7d": None,
        "daily_series": [],  # [{date, reach, reels, avg}]
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


def enrich_with_analytics(session, account):
    """Fetch analytics + history and add computed stats to account dict."""
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

    history = fetch_history_posts(session, username, pages=3)
    reels_per_day = compute_reels_per_day(history)

    daily_series = compute_avg_reach_per_reel(timeseries, reels_per_day)
    account["daily_series"] = daily_series

    # 7-day window
    today = datetime.now(timezone.utc).date()
    cutoff_7d = (today - timedelta(days=7)).isoformat()
    recent = [d for d in daily_series if d["date"] >= cutoff_7d]
    account["reach_7d"] = int(sum(d["reach"] for d in recent))
    account["reels_7d"] = sum(d["reels"] for d in recent)
    account["reach_28d"] = int(sum(d["reach"] for d in daily_series))

    avgs = [d["avg"] for d in recent if d["avg"] is not None]
    account["avg_reach_per_reel_7d"] = round(sum(avgs) / len(avgs), 1) if avgs else None

    return account


def compute_global_series(accounts):
    """Sum daily reach across all accounts to produce a global timeseries."""
    totals = defaultdict(lambda: {"reach": 0, "reels": 0})
    for acc in accounts:
        for d in acc.get("daily_series", []):
            totals[d["date"]]["reach"] += d["reach"]
            totals[d["date"]]["reels"] += d["reels"]

    result = []
    for date_str in sorted(totals.keys()):
        reach = totals[date_str]["reach"]
        reels = totals[date_str]["reels"]
        avg = round(reach / reels, 1) if reels > 0 and reach > 0 else None
        result.append({"date": date_str, "reach": reach, "reels": reels, "avg": avg})
    return result


def main():
    api_key = os.environ.get("API_KEY", "")
    dash_password = os.environ.get("DASH_PASSWORD", "IGdash2026!")

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
    print(f"Found {len(profiles)} profiles. Running deep token checks + analytics in parallel...")

    # Phase 1: status checks (deep token) in parallel
    accounts = []
    with ThreadPoolExecutor(max_workers=min(len(profiles), 20)) as pool:
        futures = {pool.submit(check_account, session, p, url_map): p for p in profiles}
        for future in as_completed(futures):
            try:
                accounts.append(future.result())
            except Exception as e:
                print(f"Error checking account: {e}", file=sys.stderr)

    # Phase 2: analytics + history enrichment for active accounts in parallel
    active_accounts = [a for a in accounts if a["status"] == "ACTIVE"]
    inactive_accounts = [a for a in accounts if a["status"] != "ACTIVE"]

    print(f"Fetching analytics for {len(active_accounts)} active accounts...")
    with ThreadPoolExecutor(max_workers=min(len(active_accounts), 15)) as pool:
        futures = {pool.submit(enrich_with_analytics, session, a): a for a in active_accounts}
        for future in as_completed(futures):
            try:
                future.result()  # enrichment mutates in place
            except Exception as e:
                print(f"Error enriching account: {e}", file=sys.stderr)

    all_accounts = active_accounts + inactive_accounts
    all_accounts.sort(key=lambda a: a["username"])

    # Global aggregates
    global_series = compute_global_series(active_accounts)
    today = datetime.now(timezone.utc).date()
    cutoff_7d = (today - timedelta(days=7)).isoformat()
    recent_global = [d for d in global_series if d["date"] >= cutoff_7d]
    global_reach_7d = int(sum(d["reach"] for d in recent_global))
    global_reach_28d = int(sum(d["reach"] for d in global_series))
    global_total_views = sum(a.get("total_views", 0) for a in active_accounts)
    global_reels_7d = sum(d["reels"] for d in recent_global)
    global_avgs = [d["avg"] for d in recent_global if d["avg"] is not None]
    global_avg_per_reel_7d = round(sum(global_avgs) / len(global_avgs), 1) if global_avgs else None

    # Prior 7 days for trend comparison
    cutoff_14d = (today - timedelta(days=14)).isoformat()
    prior_global = [d for d in global_series if cutoff_14d <= d["date"] < cutoff_7d]
    prior_reach_7d = int(sum(d["reach"] for d in prior_global))
    trend = "up" if global_reach_7d > prior_reach_7d else ("down" if global_reach_7d < prior_reach_7d else "flat")

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
            "reach_7d": global_reach_7d,
            "reach_28d": global_reach_28d,
            "reels_7d": global_reels_7d,
            "avg_reach_per_reel_7d": global_avg_per_reel_7d,
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
    print(f"Global 7d reach: {global_reach_7d:,} | avg/reel: {global_avg_per_reel_7d} | trend: {trend}")
    print(f"Updated at: {updated_at_display}")


if __name__ == "__main__":
    main()
