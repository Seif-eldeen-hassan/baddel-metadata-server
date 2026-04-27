import argparse
import html
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


JUNK_RE = re.compile(
    r"\b("
    r"demo|playtest|test server|dedicated server|server|"
    r"soundtrack|ost|sdk|editor|benchmark|trailer|teaser|"
    r"dlc|season pass"
    r")\b",
    re.IGNORECASE,
)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 BaddelMetadataBot/1.0",
    "Accept": "application/json,text/plain,*/*",
}


def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""

    text = html.unescape(raw_html)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\t", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_description(raw_html: str) -> str:
    if not raw_html:
        return ""

    text = html.unescape(raw_html)
    text = re.sub(
        r"<(br|/p|/h[1-6]|/div|/li)\s*/?>",
        "\n",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\t", " ")
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"[ ]{2,}", " ", text)
    return text.strip()


def extract_year(date_text: str) -> str:
    if not date_text:
        return ""

    match = re.search(r"(19|20)\d{2}", date_text)
    return match.group(0) if match else ""


def clean_languages(raw_languages: str) -> List[str]:
    if not raw_languages:
        return []

    text = clean_html(raw_languages)
    text = text.replace("*", "")

    text = re.sub(
        r"languages with full audio support.*$",
        "",
        text,
        flags=re.IGNORECASE,
    )

    langs = [x.strip() for x in text.split(",") if x.strip()]
    return langs


def parse_requirements(raw_text: str) -> Dict[str, str]:
    if not raw_text:
        return {}

    result: Dict[str, str] = {}

    field_patterns = [
        ("OS", r"^(?:OS|Operating System)\s*:\s*(.+)$"),
        ("Processor", r"^(?:Processor|CPU)\s*:\s*(.+)$"),
        ("Memory", r"^(?:Memory|RAM)\s*:\s*(.+)$"),
        ("Graphics", r"^(?:Graphics|Video Card|GPU)\s*:\s*(.+)$"),
        ("Direct X", r"^(?:DirectX|Direct X|DirectX®)\s*:\s*(.+)$"),
        ("Storage", r"^(?:Storage|Hard Drive|Hard Disk Space|Disk Space)\s*:\s*(.+)$"),
        ("Sound Card", r"^(?:Sound Card|Sound)\s*:\s*(.+)$"),
        ("Network", r"^(?:Network|Broadband Internet connection)\s*:\s*(.+)$"),
    ]

    for line in raw_text.splitlines():
        line = line.strip(" -•\t")
        if not line:
            continue

        for key, pattern in field_patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                value = value.replace(" RAM", "").replace(" available space", "")
                value = value.replace("Version ", "")
                result[key] = value
                break

    return result


def safe_list_descriptions(items: Any) -> List[str]:
    if not isinstance(items, list):
        return []

    output = []
    for item in items:
        if isinstance(item, dict) and item.get("description"):
            output.append(item["description"])

    return output


def get_json(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    timeout: int = 25,
) -> Tuple[Optional[Any], Dict[str, Any]]:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, params=params, timeout=timeout)
            debug = {
                "url": response.url,
                "status_code": response.status_code,
                "text_preview": response.text[:500],
            }

            if not response.ok:
                last_error = {
                    **debug,
                    "error": f"HTTP {response.status_code}",
                    "attempt": attempt,
                }
                time.sleep(1.5 * attempt)
                continue

            try:
                return response.json(), debug
            except ValueError:
                last_error = {
                    **debug,
                    "error": "Response is not valid JSON",
                    "attempt": attempt,
                }
                time.sleep(1.5 * attempt)
                continue

        except requests.RequestException as exc:
            last_error = {
                "url": url,
                "params": params,
                "error": str(exc),
                "attempt": attempt,
            }
            time.sleep(1.5 * attempt)

    return None, last_error or {"error": "Unknown request error"}


def fetch_store_details(
    session: requests.Session,
    app_id: str,
    cc: str = "us",
    lang: str = "english",
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    url = "https://store.steampowered.com/api/appdetails"
    params = {
        "appids": app_id,
        "cc": cc,
        "l": lang,
    }

    payload, debug = get_json(session, url, params=params)

    if not isinstance(payload, dict):
        return None, {
            "reason": "store_api_no_json",
            "debug": debug,
        }

    entry = payload.get(str(app_id))

    if entry is None:
        return None, {
            "reason": "store_api_missing_appid_key",
            "debug": debug,
            "raw": payload,
        }

    if not entry.get("success"):
        return None, {
            "reason": "store_api_success_false",
            "debug": debug,
            "raw": entry,
        }

    data = entry.get("data")

    if not isinstance(data, dict):
        return None, {
            "reason": "store_api_missing_data",
            "debug": debug,
            "raw": entry,
        }

    return data, None


def fetch_reviews(
    session: requests.Session,
    app_id: str,
) -> Dict[str, Any]:
    url = f"https://store.steampowered.com/appreviews/{app_id}"
    params = {
        "json": 1,
        "language": "all",
        "purchase_type": "all",
        "num_per_page": 0,
    }

    payload, _debug = get_json(session, url, params=params, retries=2)

    if not isinstance(payload, dict):
        return {}

    return payload.get("query_summary", {}) or {}


def fetch_steamspy_details(
    session: requests.Session,
    app_id: str,
) -> Optional[Dict[str, Any]]:
    url = "https://steamspy.com/api.php"
    params = {
        "request": "appdetails",
        "appid": app_id,
    }

    payload, _debug = get_json(session, url, params=params, retries=2)

    if not isinstance(payload, dict):
        return None

    if not payload.get("appid") and not payload.get("name"):
        return None

    return payload


def build_requirements(data: Dict[str, Any]) -> Dict[str, Any]:
    systems: Dict[str, Any] = {}

    os_map = [
        ("Windows", "pc_requirements"),
        ("macOS", "mac_requirements"),
        ("Linux", "linux_requirements"),
    ]

    for os_name, os_key in os_map:
        reqs = data.get(os_key, {})

        if not isinstance(reqs, dict):
            continue

        minimum_raw = clean_description(reqs.get("minimum", ""))
        recommended_raw = clean_description(reqs.get("recommended", ""))

        if not minimum_raw and not recommended_raw:
            continue

        systems[os_name] = {}

        if minimum_raw:
            systems[os_name]["minimum"] = parse_requirements(minimum_raw)
            systems[os_name]["minimum_raw"] = minimum_raw

        if recommended_raw:
            systems[os_name]["recommended"] = parse_requirements(recommended_raw)
            systems[os_name]["recommended_raw"] = recommended_raw

    return systems


def build_rating(data: Dict[str, Any], reviews_data: Dict[str, Any]) -> Dict[str, Any]:
    metacritic = data.get("metacritic")

    if isinstance(metacritic, dict) and metacritic.get("score"):
        return {
            "score": metacritic.get("score", 0),
            "max_score": 100,
            "source": "Metacritic",
        }

    total_reviews = reviews_data.get("total_reviews", 0) or 0
    positive_reviews = reviews_data.get("total_positive", 0) or 0

    if total_reviews > 0:
        return {
            "score": round((positive_reviews / total_reviews) * 100),
            "max_score": 100,
            "source": "Steam Reviews",
        }

    return {
        "score": 0,
        "max_score": 100,
        "source": "No Rating",
    }


def build_baddel_format(
    app_id: str,
    data: Dict[str, Any],
    reviews_data: Dict[str, Any],
) -> Dict[str, Any]:
    release_date_str = data.get("release_date", {}).get("date", "")
    release_year = extract_year(release_date_str)

    platforms_dict = data.get("platforms", {}) or {}
    platforms = [
        platform.capitalize()
        for platform, supported in platforms_dict.items()
        if supported
    ]

    languages = clean_languages(data.get("supported_languages", ""))

    trailers = []
    for movie in data.get("movies", []) or []:
        video_url = movie.get("dash_h264", "")

        if not video_url:
            video_url = (
                movie.get("mp4", {}).get("max")
                or movie.get("webm", {}).get("max")
                or ""
            )

        if video_url or movie.get("thumbnail"):
            trailers.append(
                {
                    "video": video_url,
                    "thumbnail": movie.get("thumbnail", ""),
                }
            )

    screenshots = [
        item.get("path_full", "")
        for item in data.get("screenshots", []) or []
        if item.get("path_full")
    ]

    cleaned_long_desc = clean_description(data.get("detailed_description", ""))
    short_desc = clean_html(data.get("short_description", ""))

    final_description = cleaned_long_desc if len(cleaned_long_desc) >= 50 else short_desc

    vertical_poster = (
        f"https://shared.akamai.steamstatic.com/store_item_assets/"
        f"steam/apps/{app_id}/library_600x900_2x.jpg"
    )

    transparent_logo = (
        f"https://shared.akamai.steamstatic.com/store_item_assets/"
        f"steam/apps/{app_id}/logo.png"
    )

    return {
        "source": "steam_store",
        "source_appid": str(app_id),
        "title": data.get("name", ""),
        "type": data.get("type", ""),
        "short_description": short_desc,
        "description": final_description,
        "rating": build_rating(data, reviews_data),
        "release_date": release_date_str,
        "release_year": release_year,
        "developer": data.get("developers", []) or [],
        "publisher": data.get("publishers", []) or [],
        "platform": platforms,
        "genres": safe_list_descriptions(data.get("genres", [])),
        "features": safe_list_descriptions(data.get("categories", [])),
        "requirements": {
            "languages": languages,
            "systems": build_requirements(data),
        },
        "poster": vertical_poster,
        "header_image": data.get("header_image", ""),
        "capsule_image": data.get("capsule_image", ""),
        "capsule_imagev5": data.get("capsule_imagev5", ""),
        "background": data.get("background_raw") or data.get("background", ""),
        "logo": transparent_logo,
        "trailers": trailers,
        "screenshots": screenshots,
        "steam_reviews": {
            "review_score_desc": reviews_data.get("review_score_desc", "No Reviews"),
            "total_reviews": reviews_data.get("total_reviews", 0),
            "positive_reviews": reviews_data.get("total_positive", 0),
            "negative_reviews": reviews_data.get("total_negative", 0),
        },
    }


def build_partial_from_steamspy(
    app_id: str,
    steamspy_data: Dict[str, Any],
    seed_title: str = "",
) -> Dict[str, Any]:
    positive = steamspy_data.get("positive", 0) or 0
    negative = steamspy_data.get("negative", 0) or 0
    total = positive + negative

    if total > 0:
        score = round((positive / total) * 100)
        rating_source = "SteamSpy Reviews"
    else:
        score = 0
        rating_source = "No Rating"

    tags = steamspy_data.get("tags", {}) or {}

    return {
        "source": "steamspy_partial",
        "source_appid": str(app_id),
        "title": steamspy_data.get("name") or seed_title or f"Steam App {app_id}",
        "type": "unknown",
        "short_description": "",
        "description": "",
        "rating": {
            "score": score,
            "max_score": 100,
            "source": rating_source,
        },
        "release_date": "",
        "release_year": "",
        "developer": [],
        "publisher": [],
        "platform": [],
        "genres": list(tags.keys()),
        "features": [],
        "requirements": {
            "languages": [],
            "systems": {},
        },
        "poster": "",
        "header_image": "",
        "capsule_image": "",
        "capsule_imagev5": "",
        "background": "",
        "logo": "",
        "trailers": [],
        "screenshots": [],
        "steam_reviews": {
            "review_score_desc": "No Reviews",
            "total_reviews": total,
            "positive_reviews": positive,
            "negative_reviews": negative,
        },
        "steamspy": {
            "owners": steamspy_data.get("owners"),
            "average_forever": steamspy_data.get("average_forever"),
            "median_forever": steamspy_data.get("median_forever"),
            "price": steamspy_data.get("price"),
            "initialprice": steamspy_data.get("initialprice"),
        },
    }


def safe_filename(app_id: str, title: str) -> str:
    title = title or "unknown"
    title = re.sub(r"[^a-zA-Z0-9._-]+", "_", title).strip("_")
    title = title[:80] or "unknown"
    return f"{app_id}_{title}.json"


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=4, ensure_ascii=False)


def process_app(
    session: requests.Session,
    app_id: str,
    seed_title: str = "",
    cc: str = "us",
    lang: str = "english",
    only_games: bool = True,
    skip_coming_soon: bool = False,
    skip_junk_names: bool = True,
    save_partial: bool = False,
) -> Tuple[str, Dict[str, Any]]:
    app_id = str(app_id).strip()

    if not app_id.isdigit():
        return "rejected", {
            "id": app_id,
            "title": seed_title,
            "reason": "invalid_non_numeric_appid",
        }

    print(f"🔄 Fetching Steam Store data for AppID: {app_id}...")

    store_data, store_error = fetch_store_details(
        session=session,
        app_id=app_id,
        cc=cc,
        lang=lang,
    )

    if store_data is None:
        print(f"⚠️ Store API unavailable for {app_id}. Trying SteamSpy fallback...")

        steamspy_data = fetch_steamspy_details(session, app_id)

        rejected_payload = {
            "id": app_id,
            "title": seed_title,
            "reason": "store_details_unavailable",
            "store_error": store_error,
            "has_steamspy_fallback": bool(steamspy_data),
        }

        if save_partial and steamspy_data:
            partial = build_partial_from_steamspy(app_id, steamspy_data, seed_title)
            partial["_warning"] = (
                "Partial record only. Steam Store appdetails did not return usable data."
            )
            partial["_store_error"] = store_error
            return "partial", partial

        return "rejected", rejected_payload

    title = store_data.get("name", "") or seed_title

    if only_games and store_data.get("type") != "game":
        return "rejected", {
            "id": app_id,
            "title": title,
            "reason": "not_game",
            "steam_type": store_data.get("type"),
        }

    if skip_coming_soon and store_data.get("release_date", {}).get("coming_soon"):
        return "rejected", {
            "id": app_id,
            "title": title,
            "reason": "coming_soon",
        }

    if skip_junk_names and JUNK_RE.search(title):
        return "rejected", {
            "id": app_id,
            "title": title,
            "reason": "junk_name_filter",
        }

    reviews_data = fetch_reviews(session, app_id)
    baddel_payload = build_baddel_format(app_id, store_data, reviews_data)

    return "ok", baddel_payload


def load_seed_items(input_path: Path) -> List[Dict[str, str]]:
    text = input_path.read_text(encoding="utf-8").strip()

    if not text:
        return []

    if input_path.suffix.lower() == ".json":
        data = json.loads(text)

        items = []

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    app_id = item.get("id") or item.get("appid") or item.get("app_id")
                    title = item.get("title") or item.get("name") or ""
                    if app_id:
                        items.append({"id": str(app_id).strip(), "title": str(title)})
                elif isinstance(item, (str, int)):
                    items.append({"id": str(item).strip(), "title": ""})

        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    app_id = value.get("id") or value.get("appid") or key
                    title = value.get("title") or value.get("name") or ""
                    items.append({"id": str(app_id).strip(), "title": str(title)})
                else:
                    items.append({"id": str(key).strip(), "title": ""})

        return items

    items = []

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        match = re.search(r"\b\d{2,10}\b", line)
        if match:
            items.append({"id": match.group(0), "title": ""})

    return items


def run_single_or_interactive(args: argparse.Namespace) -> None:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    out_dir = Path(args.out)
    rejected_path = Path(args.rejected)

    rejected_items = []

    while True:
        if args.appid:
            raw_app_id = args.appid
        else:
            raw_app_id = input("Enter Steam AppID or 'exit': ").strip()

        if raw_app_id.lower() == "exit":
            break

        status, payload = process_app(
            session=session,
            app_id=raw_app_id,
            cc=args.cc,
            lang=args.lang,
            only_games=not args.include_non_games,
            skip_coming_soon=args.skip_coming_soon,
            skip_junk_names=not args.include_junk_names,
            save_partial=args.save_partial,
        )

        if status in {"ok", "partial"}:
            filename = safe_filename(payload["source_appid"], payload.get("title", ""))
            output_path = out_dir / filename
            write_json(output_path, payload)

            if status == "ok":
                print(f"✅ Saved: {output_path}")
            else:
                print(f"⚠️ Saved partial record: {output_path}")

        else:
            rejected_items.append(payload)
            write_json(rejected_path, rejected_items)
            print(f"❌ Rejected {raw_app_id}: {payload.get('reason')}")

        if args.appid:
            break


def run_bulk(args: argparse.Namespace) -> None:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    input_path = Path(args.input)
    out_dir = Path(args.out)
    rejected_path = Path(args.rejected)
    combined_path = Path(args.combined)

    items = load_seed_items(input_path)

    if args.limit:
        items = items[: args.limit]

    print(f"🚀 Loaded {len(items)} AppIDs from {input_path}")

    accepted = []
    rejected = []

    for index, item in enumerate(items, start=1):
        app_id = item["id"]
        seed_title = item.get("title", "")

        print(f"\n[{index}/{len(items)}] Processing {app_id} {seed_title}".strip())

        status, payload = process_app(
            session=session,
            app_id=app_id,
            seed_title=seed_title,
            cc=args.cc,
            lang=args.lang,
            only_games=not args.include_non_games,
            skip_coming_soon=args.skip_coming_soon,
            skip_junk_names=not args.include_junk_names,
            save_partial=args.save_partial,
        )

        if status in {"ok", "partial"}:
            filename = safe_filename(payload["source_appid"], payload.get("title", ""))
            output_path = out_dir / filename
            write_json(output_path, payload)
            accepted.append(payload)

            if status == "ok":
                print(f"✅ Saved: {output_path}")
            else:
                print(f"⚠️ Saved partial record: {output_path}")

        else:
            rejected.append(payload)
            print(f"❌ Rejected {app_id}: {payload.get('reason')}")

        write_json(combined_path, accepted)
        write_json(rejected_path, rejected)

        if args.delay > 0 and index < len(items):
            time.sleep(args.delay)

    print("\nDone.")
    print(f"✅ Accepted: {len(accepted)}")
    print(f"❌ Rejected: {len(rejected)}")
    print(f"📦 Combined JSON: {combined_path}")
    print(f"🧾 Rejected JSON: {rejected_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Baddel Steam metadata parser with validation and fallback."
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--appid", help="Single Steam AppID to fetch.")
    mode.add_argument("--input", help="Input seed file: JSON/TXT with AppIDs.")

    parser.add_argument("--out", default="data/steam_games", help="Output folder.")
    parser.add_argument(
        "--combined",
        default="data/steam_games_combined.json",
        help="Combined accepted games JSON path.",
    )
    parser.add_argument(
        "--rejected",
        default="data/steam_rejected.json",
        help="Rejected/unavailable AppIDs JSON path.",
    )

    parser.add_argument("--cc", default="us", help="Steam country code.")
    parser.add_argument("--lang", default="english", help="Steam language.")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between bulk requests.")
    parser.add_argument("--limit", type=int, default=0, help="Limit bulk processing count.")

    parser.add_argument(
        "--save-partial",
        action="store_true",
        help="Save SteamSpy partial record when Store API fails.",
    )
    parser.add_argument(
        "--include-non-games",
        action="store_true",
        help="Do not reject apps where Steam type is not 'game'.",
    )
    parser.add_argument(
        "--include-junk-names",
        action="store_true",
        help="Do not reject demo/server/soundtrack/DLC-like names.",
    )
    parser.add_argument(
        "--skip-coming-soon",
        action="store_true",
        help="Reject games marked as coming soon.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.input:
        run_bulk(args)
    else:
        run_single_or_interactive(args)


if __name__ == "__main__":
    main()