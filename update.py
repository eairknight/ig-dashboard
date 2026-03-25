"""
IG Status Dashboard updater.
Reads API_KEY and DASH_PASSWORD from environment variables.
Writes data.json with current account statuses.
Run by GitHub Actions daily.
"""

import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pytz
import requests
from requests.adapters import HTTPAdapter

API_BASE = "https://api.upload-post.com"
USERS_URL = f"{API_BASE}/api/uploadposts/users"
MEDIA_URL = f"{API_BASE}/api/uploadposts/media"

DATA_FILE = os.path.join(os.path.dirname(__file__), "data.json")


def make_session(api_key):
    s = requests.Session()
    s.headers["Authorization"] = f"Apikey {api_key}"
    adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


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


def check_account(session, profile):
    username = profile.get("username", "?")
    ig = profile.get("social_accounts", {}).get("instagram")
    blocked = profile.get("blocked", False)

    if blocked:
        return {
            "username": username,
            "ig_handle": "",
            "display_name": "",
            "profile_pic": "",
            "status": "BLOCKED",
            "reauth_required": False,
            "blocked": True,
            "error_msg": None,
        }

    if not ig:
        return {
            "username": username,
            "ig_handle": "",
            "display_name": "",
            "profile_pic": "",
            "status": "NO_IG",
            "reauth_required": False,
            "blocked": False,
            "error_msg": None,
        }

    ig_handle = ig.get("handle", "")
    display_name = ig.get("display_name", "")
    profile_pic = ig.get("social_images", "")
    reauth_required = ig.get("reauth_required", False)

    if reauth_required:
        return {
            "username": username,
            "ig_handle": ig_handle,
            "display_name": display_name,
            "profile_pic": profile_pic,
            "status": "REAUTH",
            "reauth_required": True,
            "blocked": False,
            "error_msg": "Reconnect required in Upload-Post dashboard",
        }

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

    status = classify_status(ig, deep_ok, deep_error)

    return {
        "username": username,
        "ig_handle": ig_handle,
        "display_name": display_name,
        "profile_pic": profile_pic,
        "status": status,
        "reauth_required": reauth_required,
        "blocked": False,
        "error_msg": deep_error if not deep_ok else None,
    }


def main():
    api_key = os.environ.get("API_KEY", "")
    dash_password = os.environ.get("DASH_PASSWORD", "IGdash2026!")

    if not api_key:
        print("ERROR: API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    pass_hash = hashlib.sha256(dash_password.encode()).hexdigest()

    session = make_session(api_key)

    print("Fetching profiles...")
    resp = session.get(USERS_URL, timeout=30)
    resp.raise_for_status()
    profiles = resp.json().get("profiles", [])
    print(f"Found {len(profiles)} profiles. Deep-checking tokens in parallel...")

    accounts = []
    with ThreadPoolExecutor(max_workers=min(len(profiles), 20)) as pool:
        futures = {pool.submit(check_account, session, p): p for p in profiles}
        for future in as_completed(futures):
            try:
                accounts.append(future.result())
            except Exception as e:
                print(f"Error checking account: {e}", file=sys.stderr)

    accounts.sort(key=lambda a: a["username"])

    summary = {s: 0 for s in ("ACTIVE", "REAUTH", "CHECKPOINT", "BROKEN", "NO_IG", "BLOCKED")}
    for a in accounts:
        summary[a["status"]] = summary.get(a["status"], 0) + 1

    now_utc = datetime.now(timezone.utc)
    est = pytz.timezone("US/Eastern")
    now_est = now_utc.astimezone(est)
    updated_at_display = now_est.strftime("%A, %b %-d at %-I:%M %p EST")

    data = {
        "updated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_at_display": updated_at_display,
        "pass_hash": pass_hash,
        "summary": summary,
        "accounts": accounts,
    }

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Wrote {DATA_FILE}")
    print(f"Summary: {summary}")
    print(f"Updated at: {updated_at_display}")


if __name__ == "__main__":
    main()
