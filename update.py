"""
IG Status Dashboard updater.
Reads API_KEY and DASH_PASSWORD from environment variables.
Writes data.json with current account statuses, analytics, and performance metrics.
Run by GitHub Actions daily.
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
POST_ANALYTICS_URL = f"{API_BASE}/api/uploadposts/post-analytics"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data.json")
AUTH_FILE = os.path.join(BASE_DIR, "auth.json")
URL_MAP_FILES = [
    os.path.join(BASE_DIR, "account_url_map.json"),
    os.path.join(BASE_DIR, "..", "account_url_map.json"),
]


def make_session(api_key):
    s = requests.Session()
    s.headers["Authorization"] = f"Apikey {api_key}"
    adapter = HTTPAdapter(pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def load_url_map():
    for candidate in URL_MAP_FILES:
        path = os.path.normpath(candidate)
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
            "followers": ig.get("followers", 0) or 0,
            "timeseries": [
                {"date": d["date"], "reach": d["value"]}
                for d in timeseries
            ],
        }
    except Exception:
        return None


def fetch_instagram_reel_history(session, max_pages=20, page_size=100):
    """
    Fetch recent successful Instagram video posts from upload history.
    Returns items ordered newest->oldest as provided by the API.
    """
    reels = []
    for page in range(1, max_pages + 1):
        try:
            r = session.get(
                HISTORY_URL,
                params={"page": page, "limit": page_size},
                timeout=30,
            )
            if r.status_code != 200:
                break
            data = r.json()
            items = data.get("history", [])
            if not items:
                break
            for item in items:
                if item.get("platform") != "instagram":
                    continue
                if item.get("media_type") != "video":
                    continue
                if item.get("success") is not True:
                    continue
                if not item.get("request_id"):
                    continue
                reels.append(item)
            if len(items) < page_size:
                break
        except Exception:
            break
    return reels


def extract_post_views(payload):
    """
    Extract a per-post views value from post-analytics responses.
    The response can vary by endpoint version, so we try several shapes.
    """
    candidates = []
    if isinstance(payload, dict):
        candidates.append(payload)
        ig = payload.get("instagram")
        if isinstance(ig, dict):
            candidates.append(ig)
        data = payload.get("data")
        if isinstance(data, dict):
            candidates.append(data)
            data_ig = data.get("instagram")
            if isinstance(data_ig, dict):
                candidates.append(data_ig)
        platforms = payload.get("platforms")
        if isinstance(platforms, dict):
            candidates.append(platforms)
            p_ig = platforms.get("instagram")
            if isinstance(p_ig, dict):
                candidates.append(p_ig)

    metrics = []
    for c in candidates:
        pm = c.get("post_metrics")
        if isinstance(pm, dict):
            metrics.append(pm)
        elif isinstance(pm, list):
            metrics.extend(x for x in pm if isinstance(x, dict))
        # Some payloads may expose post fields directly.
        metrics.append(c)

    for m in metrics:
        for key in ("views", "view_count", "impressions", "plays"):
            v = m.get(key) if isinstance(m, dict) else None
            if isinstance(v, (int, float)):
                return max(0, int(round(v)))
    return None


def fetch_post_views(session, request_id):
    """Fetch per-post Instagram views for one request_id."""
    try:
        r = session.get(
            f"{POST_ANALYTICS_URL}/{request_id}",
            params={"platform": "instagram"},
            timeout=30,
        )
        if r.status_code != 200:
            return None
        return extract_post_views(r.json())
    except Exception:
        return None


def compute_shadowban_health(session, active_accounts):
    """
    Heuristic detector:
    - Look at the most recent 10 Instagram reels per account.
    - Flag AT_RISK if >=3 reels have 0 views.
    """
    checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    history = fetch_instagram_reel_history(session)

    by_profile = defaultdict(list)
    for item in history:
        profile = item.get("profile_username")
        if profile:
            by_profile[profile].append(item)

    # Cache request_id lookups so each post is only queried once.
    views_cache = {}

    def health_for_account(account):
        username = account["username"]
        recent = by_profile.get(username, [])[:10]
        if not recent:
            account["shadowban_health"] = {
                "status": "INSUFFICIENT_DATA",
                "zero_views_count": 0,
                "sample_size": 0,
                "latest_views": None,
                "checked_at": checked_at,
            }
            return

        ordered_request_ids = [item.get("request_id") for item in recent if item.get("request_id")]
        views = []
        for rid in ordered_request_ids:
            if rid not in views_cache:
                views_cache[rid] = fetch_post_views(session, rid)
            views.append(views_cache[rid])

        available = [v for v in views if isinstance(v, int)]
        sample_size = len(available)
        zero_views = sum(1 for v in available if v == 0)
        latest_views = next((v for v in views if isinstance(v, int)), None)

        if sample_size == 0:
            status = "UNAVAILABLE"
        elif sample_size < 10:
            status = "INSUFFICIENT_DATA"
        elif zero_views >= 3:
            status = "AT_RISK"
        else:
            status = "HEALTHY"

        account["shadowban_health"] = {
            "status": status,
            "zero_views_count": zero_views,
            "sample_size": sample_size,
            "latest_views": latest_views,
            "checked_at": checked_at,
        }

    with ThreadPoolExecutor(max_workers=min(len(active_accounts), 20)) as pool:
        futures = [pool.submit(health_for_account, account) for account in active_accounts]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                pass


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
        "daily_series": [],
        "shadowban_health": {
            "status": "UNAVAILABLE",
            "zero_views_count": 0,
            "sample_size": 0,
            "latest_views": None,
            "checked_at": None,
        },
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
    """Fetch analytics timeseries and compute per-account stats."""
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

    return account


def compute_global_series(accounts):
    """
    Sum daily reach across all active accounts and add a derived average line.
    avg = daily_reach / number_of_active_accounts
    """
    totals = defaultdict(int)
    for acc in accounts:
        for d in acc.get("daily_series", []):
            totals[d["date"]] += d["reach"]

    active_count = max(len(accounts), 1)
    return [
        {
            "date": date_str,
            "reach": totals[date_str],
            "avg": round(totals[date_str] / active_count, 2),
        }
        for date_str in sorted(totals.keys())
    ]


def main():
    api_key = os.environ.get("API_KEY", "")
    dash_password = os.environ.get("DASH_PASSWORD", "")

    if not api_key:
        print("ERROR: API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)
    if not dash_password:
        print("ERROR: DASH_PASSWORD environment variable not set.", file=sys.stderr)
        sys.exit(1)

    pass_hash = hashlib.sha256(dash_password.encode()).hexdigest()
    url_map = load_url_map()
    session = make_session(api_key)

    print("Fetching profiles...")
    resp = session.get(USERS_URL, timeout=30)
    resp.raise_for_status()
    profiles = resp.json().get("profiles", [])
    print(f"Found {len(profiles)} profiles.")

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

    num_active = len(active_accounts) or 1

    # Phase 2: analytics enrichment in parallel
    print(f"Fetching analytics for {num_active} active accounts...")
    with ThreadPoolExecutor(max_workers=min(num_active, 15)) as pool:
        futures = {
            pool.submit(enrich_with_analytics, session, a): a
            for a in active_accounts
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error enriching account: {e}", file=sys.stderr)

    print("Computing shadowban health (recent post views)...")
    compute_shadowban_health(session, active_accounts)

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
        "summary": summary,
        "global": {
            "total_views": global_total_views,
            "reach_7d": global_reach_7d,
            "reach_28d": global_reach_28d,
            "trend": trend,
            "trend_pct": round((global_reach_7d - prior_reach_7d) / prior_reach_7d * 100, 1) if prior_reach_7d > 0 else None,
            "series": global_series,
        },
        "accounts": all_accounts,
    }

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    with open(AUTH_FILE, "w", encoding="utf-8") as f:
        json.dump({"pass_hash": pass_hash}, f, indent=2, ensure_ascii=False)

    print(f"Wrote {DATA_FILE}")
    print(f"Wrote {AUTH_FILE}")
    print(f"Summary: {summary}")
    print(f"Global 7d reach: {global_reach_7d:,} | trend: {trend}")
    print(f"Updated at: {updated_at_display}")


if __name__ == "__main__":
    main()
