"""
baddel_steam_fetcher.py

Drop-in replacement for Baddel metadata server.

Usage from Node bridge:
    python baddel_steam_fetcher.py <steam_appid>

Important integration rule:
    stdout must contain exactly one JSON object.
    Any debug/log output goes to stderr only.
"""

from __future__ import annotations

import html
import json
import os
import re
import sys
import time
from typing import Any, Optional, Tuple

import requests


STORE_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"
STORE_REVIEWS_URL = "https://store.steampowered.com/appreviews/{appid}"
STEAMSPY_URL = "https://steamspy.com/api.php"

DEFAULT_CC = os.getenv("STEAM_CC", "us")
DEFAULT_LANG = os.getenv("STEAM_LANG", "english")
ONLY_GAMES = os.getenv("BADDDEL_STEAM_ONLY_GAME", "0").lower() in {"1", "true", "yes"}
DEBUG = os.getenv("STEAM_FETCHER_DEBUG", "0").lower() in {"1", "true", "yes"}

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 BaddelMetadataBot/2.0 (+https://baddel.gg)",
    "Accept": "application/json,text/plain,*/*",
}

REQ_KEY_MAP = {
    "os": "os_versions",
    "operating system": "os_versions",
    "processor": "cpu",
    "cpu": "cpu",
    "memory": "ram",
    "ram": "ram",
    "graphics": "gpu",
    "gpu": "gpu",
    "video card": "gpu",
    "storage": "storage",
    "hard drive": "storage",
    "hard disk space": "storage",
    "disk space": "storage",
    "directx": "directx",
    "direct x": "directx",
    "network": "notes",
    "sound card": "notes",
    "sound": "notes",
    "additional notes": "notes",
    "notes": "notes",
}

JUNK_NAME_RE = re.compile(
    r"\b(demo|playtest|test server|dedicated server|server|soundtrack|ost|sdk|editor|benchmark|trailer|teaser|season pass)\b",
    re.IGNORECASE,
)


def _debug(message: str) -> None:
    if DEBUG:
        print(f"[steam-fetcher] {message}", file=sys.stderr)


def emit_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def normalise_appid(value: Any) -> str:
    return str(value or "").strip()


def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    text = html.unescape(str(raw_html))
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_description(raw_html: str) -> str:
    if not raw_html:
        return ""
    text = html.unescape(str(raw_html))
    text = re.sub(r"<(br|/p|/h[1-6]|/div|/li)\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\t", " ").replace("\r", "")
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"[ ]{2,}", " ", text)
    return text.strip()


def clean_languages(raw_languages: str) -> list[str]:
    if not raw_languages:
        return []
    text = clean_html(raw_languages).replace("*", "")
    text = re.sub(r"languages with full audio support.*$", "", text, flags=re.IGNORECASE).strip()
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def extract_year(date_text: str) -> str:
    if not date_text:
        return ""
    match = re.search(r"(?:19|20)\d{2}", str(date_text))
    return match.group(0) if match else ""


def _plain_req_lines(html_str: str) -> list[str]:
    text = html.unescape(str(html_str or ""))

    # Preserve boundaries from common Steam requirement markup.
    text = re.sub(r"<(br|/li|/p|/div|/ul|/h[1-6])\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</strong>\s*", ": ", text, flags=re.IGNORECASE)
    text = re.sub(r"<strong[^>]*>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\r", "")

    lines: list[str] = []
    for raw in text.splitlines():
        line = re.sub(r"\s+", " ", raw).strip(" -•\t:")
        if line:
            lines.append(line)
    return lines


def parse_steam_requirements(html_str: str) -> dict[str, str]:
    """Parse Steam requirement HTML into DB field names used by enrichGame.js."""
    if not html_str or not isinstance(html_str, str):
        return {}

    parsed: dict[str, str] = {}
    note_parts: list[str] = []

    for line in _plain_req_lines(html_str):
        lower = line.lower().strip()

        if lower in {"minimum", "recommended"}:
            continue

        if lower.startswith("requires ") or "64-bit processor" in lower:
            note_parts.append(line)
            continue

        # Accept both "OS: Windows" and occasional "OS Windows" shapes.
        match = re.match(r"^([A-Za-z][A-Za-z /®+.-]{1,40})\s*:\s*(.+)$", line)
        if not match:
            continue

        raw_key = match.group(1).strip().lower().replace("®", "")
        value = match.group(2).strip()
        value = value.replace(" RAM", "").replace(" available space", "")
        value = value.replace("Version ", "")
        value = re.sub(r"\s+", " ", value).strip()
        if not value:
            continue

        db_key = REQ_KEY_MAP.get(raw_key)
        if not db_key:
            continue

        if db_key == "notes":
            note_parts.append(value)
        else:
            parsed[db_key] = value

    if note_parts:
        parsed["notes"] = " | ".join(dict.fromkeys(note_parts))

    return parsed


def safe_descriptions(items: Any) -> list[str]:
    if not isinstance(items, list):
        return []
    output: list[str] = []
    for item in items:
        if isinstance(item, dict) and item.get("description"):
            output.append(str(item["description"]))
    return output


def request_json(
    session: requests.Session,
    url: str,
    *,
    params: Optional[dict[str, Any]] = None,
    timeout: int = 20,
    retries: int = 3,
) -> Tuple[Optional[Any], dict[str, Any]]:
    last_debug: dict[str, Any] = {"url": url, "params": params}

    for attempt in range(1, retries + 1):
        try:
            res = session.get(url, params=params, timeout=timeout)
            preview = (res.text or "")[:500]
            last_debug = {
                "url": res.url,
                "status_code": res.status_code,
                "content_type": res.headers.get("Content-Type", ""),
                "text_preview": preview,
                "attempt": attempt,
            }

            if res.status_code == 429:
                retry_after = res.headers.get("Retry-After")
                wait_s = 1.5 * attempt
                if retry_after and retry_after.isdigit():
                    wait_s = min(float(retry_after), 8.0)
                _debug(f"429 from {res.url}; sleeping {wait_s:.1f}s")
                time.sleep(wait_s)
                continue

            if not res.ok:
                time.sleep(0.75 * attempt)
                continue

            try:
                return res.json(), last_debug
            except ValueError:
                last_debug["error"] = "non_json_response"
                time.sleep(0.75 * attempt)
                continue

        except requests.RequestException as exc:
            last_debug = {"url": url, "params": params, "error": str(exc), "attempt": attempt}
            time.sleep(0.75 * attempt)

    return None, last_debug


def fetch_store_details(
    session: requests.Session,
    app_id: str,
    *,
    cc: str = DEFAULT_CC,
    lang: str = DEFAULT_LANG,
) -> Tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    payload, debug = request_json(
        session,
        STORE_APPDETAILS_URL,
        params={"appids": app_id, "cc": cc, "l": lang},
        timeout=20,
        retries=3,
    )

    if not isinstance(payload, dict):
        return None, {"reason": "store_api_no_json", "debug": debug}

    entry = payload.get(app_id)
    if entry is None:
        return None, {"reason": "store_api_missing_appid_key", "debug": debug}

    if not entry.get("success"):
        return None, {"reason": "store_api_success_false", "debug": debug, "raw": entry}

    data = entry.get("data")
    if not isinstance(data, dict) or not data:
        return None, {"reason": "store_api_missing_data", "debug": debug, "raw": entry}

    return data, None


def fetch_reviews(session: requests.Session, app_id: str) -> dict[str, Any]:
    payload, _debug_payload = request_json(
        session,
        STORE_REVIEWS_URL.format(appid=app_id),
        params={
            "json": 1,
            "language": "all",
            "purchase_type": "all",
            "num_per_page": 0,
        },
        timeout=15,
        retries=2,
    )
    if not isinstance(payload, dict):
        return {}
    return payload.get("query_summary", {}) or {}


def fetch_steamspy_details(session: requests.Session, app_id: str) -> Optional[dict[str, Any]]:
    payload, _debug_payload = request_json(
        session,
        STEAMSPY_URL,
        params={"request": "appdetails", "appid": app_id},
        timeout=15,
        retries=2,
    )
    if not isinstance(payload, dict):
        return None
    if not payload.get("appid") and not payload.get("name"):
        return None
    return payload


def build_requirements(data: dict[str, Any]) -> dict[str, Any]:
    systems: dict[str, Any] = {}
    for os_name, os_key in [
        ("Windows", "pc_requirements"),
        ("macOS", "mac_requirements"),
        ("Linux", "linux_requirements"),
    ]:
        reqs = data.get(os_key) or {}
        if not isinstance(reqs, dict):
            continue

        minimum_html = reqs.get("minimum", "") or ""
        recommended_html = reqs.get("recommended", "") or ""
        if not minimum_html and not recommended_html:
            continue

        tiers: dict[str, Any] = {}
        if minimum_html:
            tiers["minimum"] = parse_steam_requirements(minimum_html)
        if recommended_html:
            tiers["recommended"] = parse_steam_requirements(recommended_html)

        # Avoid saving completely empty tier objects.
        tiers = {tier: values for tier, values in tiers.items() if values}
        if tiers:
            systems[os_name] = tiers

    return systems


def build_rating(data: dict[str, Any], reviews: dict[str, Any]) -> dict[str, Any]:
    metacritic = data.get("metacritic")
    if isinstance(metacritic, dict) and metacritic.get("score"):
        return {"score": metacritic.get("score", 0), "max_score": 100, "source": "Metacritic"}

    total = int(reviews.get("total_reviews") or 0)
    positive = int(reviews.get("total_positive") or 0)
    if total > 0:
        return {"score": round((positive / total) * 100, 1), "max_score": 100, "source": "Steam Reviews"}

    return {"score": 0, "max_score": 100, "source": "No Rating"}


def static_steam_assets(app_id: str) -> dict[str, str]:
    return {
        "poster": f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{app_id}/library_600x900_2x.jpg",
        "logo": f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{app_id}/logo.png",
        "header_image": f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{app_id}/header.jpg",
        "capsule_image": f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{app_id}/capsule_616x353.jpg",
        "background": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/page_bg_generated_v6b.jpg",
    }


def build_from_store(app_id: str, data: dict[str, Any], reviews: dict[str, Any]) -> dict[str, Any]:
    release_date = ""
    if isinstance(data.get("release_date"), dict):
        release_date = data.get("release_date", {}).get("date", "") or ""

    platforms_dict = data.get("platforms") or {}
    platforms = [name.capitalize() for name, supported in platforms_dict.items() if supported]

    trailers: list[dict[str, str]] = []
    for movie in data.get("movies") or []:
        if not isinstance(movie, dict):
            continue
        video_url = movie.get("dash_h264") or movie.get("mp4", {}).get("max") or movie.get("webm", {}).get("max") or ""
        thumb_url = movie.get("thumbnail", "") or ""
        if video_url or thumb_url:
            trailers.append({"video": video_url, "thumbnail": thumb_url})

    screenshots = []
    for ss in data.get("screenshots") or []:
        if isinstance(ss, dict) and ss.get("path_full"):
            screenshots.append({"url": ss.get("path_full", ""), "width": 1920, "height": 1080})

    assets = static_steam_assets(app_id)
    detailed_description = clean_description(data.get("detailed_description", ""))
    short_description = clean_html(data.get("short_description", ""))
    final_description = detailed_description if len(detailed_description) >= 50 else short_description

    return {
        "source": "steam_store",
        "source_appid": app_id,
        "title": data.get("name", "") or f"Steam App {app_id}",
        "type": data.get("type", ""),
        "short_description": short_description,
        "description": final_description,
        "rating": build_rating(data, reviews),
        "release_date": release_date,
        "release_year": extract_year(release_date),
        "developer": data.get("developers") or [],
        "publisher": data.get("publishers") or [],
        "platform": platforms,
        "genres": safe_descriptions(data.get("genres")),
        "features": safe_descriptions(data.get("categories")),
        "requirements": {
            "languages": clean_languages(data.get("supported_languages", "")),
            "systems": build_requirements(data),
        },
        "poster": assets["poster"],
        "header_image": data.get("header_image") or assets["header_image"],
        "capsule_image": data.get("capsule_image") or assets["capsule_image"],
        "capsule_imagev5": data.get("capsule_imagev5", ""),
        "background": data.get("background_raw") or data.get("background") or assets["background"],
        "logo": assets["logo"],
        "trailers": trailers,
        "screenshots": screenshots,
        "steam_reviews": {
            "review_score_desc": reviews.get("review_score_desc", "No Reviews"),
            "total_reviews": int(reviews.get("total_reviews") or 0),
            "positive_reviews": int(reviews.get("total_positive") or 0),
            "negative_reviews": int(reviews.get("total_negative") or 0),
        },
    }


def build_partial_from_steamspy(app_id: str, spy: dict[str, Any], store_error: Optional[dict[str, Any]]) -> dict[str, Any]:
    positive = int(spy.get("positive") or 0)
    negative = int(spy.get("negative") or 0)
    total = positive + negative
    score = round((positive / total) * 100, 1) if total > 0 else 0

    tags = spy.get("tags") or {}
    if isinstance(tags, dict):
        genres = list(tags.keys())
    else:
        genres = []

    assets = static_steam_assets(app_id)
    title = spy.get("name") or f"Steam App {app_id}"

    return {
        "source": "steamspy_partial",
        "source_appid": app_id,
        "title": title,
        "type": "unknown",
        "short_description": "",
        "description": "",
        "rating": {
            "score": score,
            "max_score": 100,
            "source": "SteamSpy Reviews" if total > 0 else "No Rating",
        },
        "release_date": "",
        "release_year": "",
        "developer": [],
        "publisher": [],
        "platform": [],
        "genres": genres,
        "features": [],
        "requirements": {"languages": [], "systems": {}},
        "poster": assets["poster"],
        "header_image": assets["header_image"],
        "capsule_image": assets["capsule_image"],
        "capsule_imagev5": "",
        "background": assets["background"],
        "logo": assets["logo"],
        "trailers": [],
        "screenshots": [],
        "steam_reviews": {
            "review_score_desc": "No Reviews",
            "total_reviews": total,
            "positive_reviews": positive,
            "negative_reviews": negative,
        },
        "steamspy": {
            "appid": spy.get("appid") or app_id,
            "owners": spy.get("owners"),
            "average_forever": spy.get("average_forever"),
            "median_forever": spy.get("median_forever"),
            "price": spy.get("price"),
            "initialprice": spy.get("initialprice"),
        },
        "_warning": "Partial SteamSpy record. Steam Store appdetails did not return usable data.",
        "_store_error_reason": (store_error or {}).get("reason"),
    }


def get_steam_game_standardized(app_id: str) -> dict[str, Any]:
    app_id = normalise_appid(app_id)
    if not app_id.isdigit():
        return {"error": "Invalid AppID: must be numeric", "appid": app_id}

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    store_data, store_error = fetch_store_details(session, app_id)

    if store_data is not None:
        steam_type = store_data.get("type", "")
        if ONLY_GAMES and steam_type and steam_type != "game":
            return {"error": "Steam AppID is not a game", "appid": app_id, "steam_type": steam_type}

        reviews = fetch_reviews(session, app_id)
        result = build_from_store(app_id, store_data, reviews)
        if JUNK_NAME_RE.search(result.get("title", "")):
            result["_warning"] = "Title matched junk-name filter; server may choose to ignore this record."
        return result

    # Storefront appdetails can return success=false for real AppIDs, delisted apps,
    # region-blocked apps, or old Rockstar-style entries. Keep a useful partial
    # record instead of throwing away the ID entirely.
    spy = fetch_steamspy_details(session, app_id)
    if spy:
        return build_partial_from_steamspy(app_id, spy, store_error)

    return {
        "error": "Steam Store appdetails unavailable and SteamSpy fallback failed",
        "appid": app_id,
        "store_error": store_error,
    }


def main() -> None:
    if len(sys.argv) < 2:
        emit_json({"error": "No AppID provided"})
        return

    app_id = normalise_appid(sys.argv[1])
    try:
        emit_json(get_steam_game_standardized(app_id))
    except Exception as exc:  # Last-resort JSON-safe error for the Node bridge.
        import traceback

        emit_json({
            "error": str(exc),
            "trace": traceback.format_exc(),
            "appid": app_id,
        })


if __name__ == "__main__":
    main()
