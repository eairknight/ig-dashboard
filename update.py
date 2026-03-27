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
DIAGNOSTICS_LOG_FILE = os.path.join(BASE_DIR, "diagnostics_log.json")
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


def parse_iso_utc(raw):
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    except Exception:
        try:
            dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def compact_views_k(value):
    n = int(value or 0)
    if n < 1000:
        return str(n)
    return f"{int(round(n / 1000.0))}k"


def fixed_est_date_key(dt_utc):
    """Map UTC timestamp to fixed EST day key (UTC-5, no DST)."""
    if not dt_utc:
        return None
    return (dt_utc - timedelta(hours=5)).date().isoformat()


def build_daily_insight(row, expected_daily_reels, baseline_avg):
    posted = int(row.get("posted_reels", 0) or 0)
    views = int(row.get("views", 0) or 0)
    zero_reels = int(row.get("zero_view_reels", 0) or 0)
    unavailable = int(row.get("estimated_unavailable_analytics_posts", 0) or 0)
    no_post_active = int(row.get("estimated_no_post_active_accounts", 0) or 0)
    avg_per_reel = row.get("avg_views_per_reel")
    avg_num = float(avg_per_reel) if isinstance(avg_per_reel, (int, float)) else None

    posting_ratio = posted / max(expected_daily_reels, 1)
    zero_rate = (zero_reels / posted) if posted else 0.0
    unavailable_rate = (unavailable / posted) if posted else 0.0
    avg_ratio = (avg_num / baseline_avg) if (avg_num is not None and baseline_avg > 0) else 1.0

    low_post_threshold = max(1, int(round(expected_daily_reels * 0.65)))
    zero_view_threshold = 0.25
    no_post_threshold = 3
    missing_analytics_threshold = 0.2
    low_avg_ratio_threshold = 0.55

    causes = []
    if posted < low_post_threshold:
        causes.append("Low posting volume")
    if zero_rate >= zero_view_threshold and posted >= 4:
        causes.append("Zero-view spike")
    if no_post_active >= no_post_threshold:
        causes.append("Many active accounts did not post")
    if unavailable_rate >= missing_analytics_threshold and posted >= 3:
        causes.append("Missing post analytics")
    if avg_num is not None and baseline_avg > 0 and avg_ratio < low_avg_ratio_threshold:
        causes.append("Low avg views per reel")
    if not causes:
        causes.append("Stable posting and view quality")

    score = 100
    if posting_ratio < 1:
        score -= min(35, int(round((1 - posting_ratio) * 35)))
    score -= min(35, int(round(zero_rate * 45)))
    score -= min(20, int(round(unavailable_rate * 35)))
    if avg_ratio < 1:
        score -= min(20, int(round((1 - avg_ratio) * 25)))
    score = max(0, min(100, score))

    avg_txt = "—"
    if avg_num is not None:
        avg_txt = str(int(round(avg_num)))
    summary_line = (
        f"{compact_views_k(views)} total views across {posted} posted reels, "
        f"{avg_txt} avg/reel. Main drag: {' + '.join(causes[:2]).lower()}."
    )
    reason_details = {
        "Low posting volume": {
            "triggered": posted < low_post_threshold,
            "posted_reels": posted,
            "threshold_min_posted_reels": low_post_threshold,
            "expected_daily_reels": expected_daily_reels,
        },
        "Zero-view spike": {
            "triggered": (zero_rate >= zero_view_threshold and posted >= 4),
            "zero_view_reels": zero_reels,
            "posted_reels": posted,
            "zero_rate_pct": round(zero_rate * 100, 1),
            "threshold_pct": int(zero_view_threshold * 100),
            "minimum_reels_required": 4,
        },
        "Many active accounts did not post": {
            "triggered": no_post_active >= no_post_threshold,
            "no_post_active_accounts": no_post_active,
            "threshold_accounts": no_post_threshold,
        },
        "Missing post analytics": {
            "triggered": (unavailable_rate >= missing_analytics_threshold and posted >= 3),
            "unavailable_posts": unavailable,
            "posted_reels": posted,
            "unavailable_rate_pct": round(unavailable_rate * 100, 1),
            "threshold_pct": int(missing_analytics_threshold * 100),
            "minimum_reels_required": 3,
        },
        "Low avg views per reel": {
            "triggered": (avg_num is not None and baseline_avg > 0 and avg_ratio < low_avg_ratio_threshold),
            "avg_views_per_reel": None if avg_num is None else round(avg_num, 2),
            "baseline_avg_views_per_reel": round(baseline_avg, 2),
            "ratio_pct_of_baseline": None if avg_num is None or baseline_avg <= 0 else round(avg_ratio * 100, 1),
            "threshold_pct_of_baseline": int(low_avg_ratio_threshold * 100),
        },
    }

    return {
        "top_causes": causes[:4],
        "health_score": score,
        "summary_line": summary_line,
        "reason_details": reason_details,
        "source": row.get("source", "estimated"),
    }


def load_diagnostics_log():
    if not os.path.exists(DIAGNOSTICS_LOG_FILE):
        return {"version": 1, "updated_at": None, "days": {}}
    try:
        with open(DIAGNOSTICS_LOG_FILE, encoding="utf-8") as f:
            parsed = json.load(f)
        if not isinstance(parsed, dict):
            return {"version": 1, "updated_at": None, "days": {}}
        days = parsed.get("days", {})
        if not isinstance(days, dict):
            days = {}
        return {
            "version": int(parsed.get("version", 1)),
            "updated_at": parsed.get("updated_at"),
            "days": days,
        }
    except Exception:
        return {"version": 1, "updated_at": None, "days": {}}


def save_diagnostics_log(payload):
    with open(DIAGNOSTICS_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def add_logged_day_snapshot(log_payload, day_key, entry, updated_at, max_days=180):
    days = log_payload.setdefault("days", {})
    days[day_key] = entry
    if len(days) > max_days:
        for old_key in sorted(days.keys())[:-max_days]:
            days.pop(old_key, None)
    log_payload["updated_at"] = updated_at
    return log_payload


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


def compute_reel_views_kpis(session, active_accounts):
    """
    Compute per-account and global views-first KPIs from Upload-Post reel history.
    Risk heuristic:
      - Look at most recent 10 reels/account
      - AT_RISK if >=3 have 0 views
    """
    now_utc = datetime.now(timezone.utc)
    checked_at = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    history = fetch_instagram_reel_history(session)
    active_usernames = {a["username"] for a in active_accounts}

    by_profile = defaultdict(list)
    all_active_posts = []
    for item in history:
        profile = item.get("profile_username")
        if profile in active_usernames:
            dt = parse_iso_utc(item.get("upload_timestamp"))
            enriched = {
                "profile_username": profile,
                "request_id": item.get("request_id"),
                "upload_timestamp": item.get("upload_timestamp"),
                "_dt": dt,
            }
            by_profile[profile].append(enriched)
            all_active_posts.append(enriched)

    # Cache request_id lookups so each post is only queried once.
    views_cache = {}
    request_ids = list({
        p["request_id"] for p in all_active_posts
        if p.get("request_id")
    })

    def resolve_views(request_id):
        return request_id, fetch_post_views(session, request_id)

    with ThreadPoolExecutor(max_workers=min(len(request_ids), 25) or 1) as pool:
        futures = [pool.submit(resolve_views, rid) for rid in request_ids]
        for future in as_completed(futures):
            try:
                rid, views = future.result()
                views_cache[rid] = views
            except Exception:
                pass

    for p in all_active_posts:
        p["views"] = views_cache.get(p.get("request_id"))

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
            account["views_24h"] = 0
            account["views_3d"] = 0
            account["views_7d"] = 0
            account["reels_7d_count"] = 0
            account["avg_views_per_reel_7d"] = None
            account["latest_reel_views"] = None
            return

        views = [p.get("views") for p in recent]

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

        cutoff_24h = now_utc - timedelta(hours=24)
        cutoff_3d = now_utc - timedelta(days=3)
        cutoff_7d = now_utc - timedelta(days=7)
        posts_with_dt = [p for p in by_profile.get(username, []) if p.get("_dt")]

        reels_7d = [p for p in posts_with_dt if p["_dt"] >= cutoff_7d]
        reels_3d = [p for p in posts_with_dt if p["_dt"] >= cutoff_3d]
        reels_24h = [p for p in posts_with_dt if p["_dt"] >= cutoff_24h]
        reels_7d_with_views = [p for p in reels_7d if isinstance(p.get("views"), int)]

        views_7d = int(sum(p.get("views", 0) for p in reels_7d if isinstance(p.get("views"), int)))
        views_3d = int(sum(p.get("views", 0) for p in reels_3d if isinstance(p.get("views"), int)))
        views_24h = int(sum(p.get("views", 0) for p in reels_24h if isinstance(p.get("views"), int)))
        reels_7d_count = len(reels_7d)
        avg_views_per_reel_7d = round(views_7d / len(reels_7d_with_views), 2) if reels_7d_with_views else None

        account["shadowban_health"] = {
            "status": status,
            "zero_views_count": zero_views,
            "sample_size": sample_size,
            "latest_views": latest_views,
            "checked_at": checked_at,
        }
        account["views_24h"] = views_24h
        account["views_3d"] = views_3d
        account["views_7d"] = views_7d
        account["reels_7d_count"] = reels_7d_count
        account["avg_views_per_reel_7d"] = avg_views_per_reel_7d
        account["latest_reel_views"] = latest_views

    with ThreadPoolExecutor(max_workers=min(len(active_accounts), 20)) as pool:
        futures = [pool.submit(health_for_account, account) for account in active_accounts]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                pass

    cutoff_24h = now_utc - timedelta(hours=24)
    cutoff_3d = now_utc - timedelta(days=3)
    cutoff_7d = now_utc - timedelta(days=7)
    cutoff_14d = now_utc - timedelta(days=14)
    cutoff_28d = now_utc - timedelta(days=28)

    daily_views = defaultdict(lambda: {
        "views": 0,
        "reels": 0,
        "zero_view_reels": 0,
        "unavailable_posts": 0,
        "posters": set(),
        "zero_view_posters": set(),
    })
    total_views_24h = 0
    total_views_3d = 0
    total_views_7d = 0
    total_reels_7d = 0
    total_reels_7d_with_views = 0
    recent_7d_daily = 0
    prior_7d_daily = 0

    for p in all_active_posts:
        dt = p.get("_dt")
        if not dt:
            continue
        date_key = fixed_est_date_key(dt)
        if not date_key:
            continue
        bucket = daily_views[date_key]
        bucket["reels"] += 1
        profile = p.get("profile_username")
        if profile:
            bucket["posters"].add(profile)
        if dt >= cutoff_7d:
            total_reels_7d += 1
        views = p.get("views")
        if not isinstance(views, int):
            bucket["unavailable_posts"] += 1
            continue
        bucket["views"] += views
        if views == 0:
            bucket["zero_view_reels"] += 1
            if profile:
                bucket["zero_view_posters"].add(profile)
        if dt >= cutoff_24h:
            total_views_24h += views
        if dt >= cutoff_3d:
            total_views_3d += views
        if dt >= cutoff_7d:
            total_views_7d += views
            total_reels_7d_with_views += 1
            recent_7d_daily += views
        elif cutoff_14d <= dt < cutoff_7d:
            prior_7d_daily += views

    avg_views_per_reel_7d = round(total_views_7d / total_reels_7d_with_views, 2) if total_reels_7d_with_views else None
    trend = "up" if recent_7d_daily > prior_7d_daily else ("down" if recent_7d_daily < prior_7d_daily else "flat")
    trend_pct = round((recent_7d_daily - prior_7d_daily) / prior_7d_daily * 100, 1) if prior_7d_daily > 0 else None

    daily_rows = []
    for date_str in sorted(daily_views.keys()):
        dt = parse_iso_utc(f"{date_str}T00:00:00Z")
        if not dt or dt < cutoff_28d:
            continue
        reels = daily_views[date_str]["reels"]
        views = daily_views[date_str]["views"]
        posted_accounts = len(daily_views[date_str]["posters"])
        daily_rows.append({
            "date": date_str,
            "views": views,
            "reels": reels,
            "posted_reels": reels,
            "avg_views_per_reel": round(views / reels, 2) if reels else None,
            "zero_view_reels": int(daily_views[date_str]["zero_view_reels"]),
            "estimated_no_post_active_accounts": max(len(active_accounts) - posted_accounts, 0),
            "estimated_at_risk_posters": int(len(daily_views[date_str]["zero_view_posters"])),
            "estimated_unavailable_analytics_posts": int(daily_views[date_str]["unavailable_posts"]),
            "source": "estimated",
        })

    reference_rows = [row for row in daily_rows if row["date"] >= cutoff_7d.date().isoformat()]
    if not reference_rows:
        reference_rows = daily_rows
    expected_daily_reels = round(
        sum(row["posted_reels"] for row in reference_rows) / max(len(reference_rows), 1)
    )
    expected_daily_reels = max(expected_daily_reels, 1)
    avg_candidates = [row["avg_views_per_reel"] for row in reference_rows if isinstance(row["avg_views_per_reel"], (int, float))]
    baseline_avg = float(sum(avg_candidates) / len(avg_candidates)) if avg_candidates else 0.0

    daily_views_series = []
    daily_health_insights = {}
    for row in daily_rows:
        daily_views_series.append(row)
        daily_health_insights[row["date"]] = build_daily_insight(row, expected_daily_reels, baseline_avg)

    return {
        "total_reels_7d": int(total_reels_7d),
        "total_views_24h": int(total_views_24h),
        "total_views_3d": int(total_views_3d),
        "total_views_7d": int(total_views_7d),
        "avg_views_per_reel_7d": avg_views_per_reel_7d,
        "daily_views_series": daily_views_series,
        "daily_health_insights": daily_health_insights,
        "trend": trend,
        "trend_pct": trend_pct,
    }


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
        "views_24h": 0,
        "views_3d": 0,
        "views_7d": 0,
        "reels_7d_count": 0,
        "avg_views_per_reel_7d": None,
        "latest_reel_views": None,
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

    print("Computing views-first reel KPIs + shadowban health...")
    reel_kpis = compute_reel_views_kpis(session, active_accounts)

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
    reach_trend = "up" if global_reach_7d > prior_reach_7d else ("down" if global_reach_7d < prior_reach_7d else "flat")


    summary = {s: 0 for s in ("ACTIVE", "REAUTH", "CHECKPOINT", "BROKEN", "NO_IG", "BLOCKED")}
    for a in all_accounts:
        summary[a["status"]] = summary.get(a["status"], 0) + 1

    now_utc = datetime.now(timezone.utc)
    bratislava = pytz.timezone("Europe/Bratislava")
    now_local = now_utc.astimezone(bratislava)
    day = now_local.day
    hour = now_local.hour % 12 or 12
    ampm = "AM" if now_local.hour < 12 else "PM"
    tz_abbr = now_local.tzname() or "CET"
    updated_at_display = now_local.strftime(f"%A, %b {day} at {hour}:{now_local.strftime('%M')} {ampm} {tz_abbr}")

    data = {
        "updated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_at_display": updated_at_display,
        "summary": summary,
        "global": {
            "total_views": global_total_views,
            "reach_7d": global_reach_7d,
            "reach_28d": global_reach_28d,
            "trend": reel_kpis["trend"],
            "trend_pct": reel_kpis["trend_pct"],
            "reach_trend": reach_trend,
            "reach_trend_pct": round((global_reach_7d - prior_reach_7d) / prior_reach_7d * 100, 1) if prior_reach_7d > 0 else None,
            "series": global_series,
            "total_reels_7d": reel_kpis["total_reels_7d"],
            "total_views_24h": reel_kpis["total_views_24h"],
            "total_views_3d": reel_kpis["total_views_3d"],
            "total_views_7d": reel_kpis["total_views_7d"],
            "avg_views_per_reel_7d": reel_kpis["avg_views_per_reel_7d"],
            "daily_views_series": reel_kpis["daily_views_series"],
            "daily_health_insights": reel_kpis["daily_health_insights"],
        },
        "accounts": all_accounts,
    }

    diagnostics_log = load_diagnostics_log()
    series_by_date = {row["date"]: row for row in reel_kpis["daily_views_series"]}
    insight_by_date = reel_kpis["daily_health_insights"]
    today_key = now_utc.date().isoformat()
    today_series = series_by_date.get(today_key, {
        "posted_reels": 0,
        "views": 0,
        "avg_views_per_reel": None,
        "zero_view_reels": 0,
        "estimated_no_post_active_accounts": len(active_accounts),
        "estimated_at_risk_posters": 0,
        "estimated_unavailable_analytics_posts": 0,
    })
    today_insight = insight_by_date.get(today_key, build_daily_insight(
        {
            "posted_reels": 0,
            "views": 0,
            "avg_views_per_reel": None,
            "zero_view_reels": 0,
            "estimated_no_post_active_accounts": len(active_accounts),
            "estimated_at_risk_posters": 0,
            "estimated_unavailable_analytics_posts": 0,
            "source": "estimated",
        },
        1,
        0.0,
    ))

    logged_entry = {
        "source": "logged",
        "logged_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "health_score": today_insight.get("health_score"),
        "top_causes": today_insight.get("top_causes", []),
        "summary_line": today_insight.get("summary_line"),
        "reason_details": today_insight.get("reason_details", {}),
        "active_accounts": summary.get("ACTIVE", 0),
        "blocked_accounts": summary.get("BLOCKED", 0),
        "broken_accounts": summary.get("BROKEN", 0),
        "checkpoint_accounts": summary.get("CHECKPOINT", 0),
        "reauth_accounts": summary.get("REAUTH", 0),
        "posted_reels": today_series.get("posted_reels", 0),
        "views": today_series.get("views", 0),
        "avg_views_per_reel": today_series.get("avg_views_per_reel"),
        "zero_view_reels": today_series.get("zero_view_reels", 0),
        "estimated_no_post_active_accounts": today_series.get("estimated_no_post_active_accounts", 0),
        "estimated_at_risk_posters": today_series.get("estimated_at_risk_posters", 0),
        "estimated_unavailable_analytics_posts": today_series.get("estimated_unavailable_analytics_posts", 0),
    }
    diagnostics_log = add_logged_day_snapshot(
        diagnostics_log,
        today_key,
        logged_entry,
        now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    log_days = diagnostics_log.get("days", {})
    for row in data["global"]["daily_views_series"]:
        logged = log_days.get(row["date"])
        if logged:
            row["source"] = "logged"
    for date_key, insight in data["global"]["daily_health_insights"].items():
        logged = log_days.get(date_key)
        if logged:
            insight["source"] = "logged"
            insight["health_score"] = logged.get("health_score", insight.get("health_score"))
            insight["top_causes"] = logged.get("top_causes", insight.get("top_causes", []))
            insight["summary_line"] = logged.get("summary_line", insight.get("summary_line"))
            insight["reason_details"] = logged.get("reason_details", insight.get("reason_details", {}))

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    with open(AUTH_FILE, "w", encoding="utf-8") as f:
        json.dump({"pass_hash": pass_hash}, f, indent=2, ensure_ascii=False)
    save_diagnostics_log(diagnostics_log)

    print(f"Wrote {DATA_FILE}")
    print(f"Wrote {AUTH_FILE}")
    print(f"Wrote {DIAGNOSTICS_LOG_FILE}")
    print(f"Summary: {summary}")
    print(f"Global 7d reach: {global_reach_7d:,} | trend: {reach_trend}")
    print(f"Global 7d reel views: {reel_kpis['total_views_7d']:,} | reels: {reel_kpis['total_reels_7d']:,} | trend: {reel_kpis['trend']}")
    print(f"Updated at: {updated_at_display}")


if __name__ == "__main__":
    main()
