"""
baddel_epic_fetcher.py
Fetches structured game data from the Epic Games Store.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import cloudscraper
from epicstore_api import EpicGamesStoreAPI

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GRAPHQL_URL = "https://store.epicgames.com/graphql"

QUERY_HASHES = {
    "video":        "e631c6a22d716a93d05bcb023b0ef4ade869f5c2c241d88faf9187b51b282236",
    "catalog":      "ec112951b1824e1e215daecae17db4069c737295d4a697ddb9832923f93a326e",
    "home_config":  "5a922bd3e5c84b60a4f443a019ef640b05cb0ae379beb4aca4515bf9812dfcb4",
    "store_config": "6a3c3cf307f98388bbb9c6958e2d4299e1e454b58951b1903439f1b7c1f74716",
}

VALID_IMAGE_TYPES: frozenset[str] = frozenset({"Screenshot", "GalleryImage"})
TALL_IMAGE_TYPES:  frozenset[str] = frozenset({"OfferImageTall", "DieselStoreFrontTall"})
WIDE_IMAGE_TYPES:  frozenset[str] = frozenset({"DieselStoreFrontWide", "OfferImageWide"})

BAD_IMAGE_HINTS: frozenset[str] = frozenset({
    "usk", "pegi", "esrb", "cero", "rating",
    "ic1-", "icon", "portrait", "-tall-", "offerimage", "banner",
})

FEATURE_TAGS: frozenset[str] = frozenset({
    "CLOUD_SAVES", "CONTROLLER", "SINGLE_PLAYER", "MULTI_PLAYER",
    "CO_OP", "CROSS_PLATFORM", "ACHIEVEMENTS", "LEADERBOARDS",
})

OS_DISPLAY = {"windows": "Windows", "mac": "Mac", "macos": "Mac", "linux": "Linux"}

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _is_bad_image(url: str) -> bool:
    lower = url.lower()
    return any(hint in lower for hint in BAD_IMAGE_HINTS)


def _parse_date(raw: str) -> str:
    """Return the 4-digit year string, or '' on failure / placeholder dates."""
    if not raw or raw.startswith("2099"):
        return ""
    try:
        return str(datetime.fromisoformat(raw.replace("Z", "+00:00")).year)
    except ValueError:
        return raw[:4]


def _extract_tags(tags: list[dict]) -> tuple[list[str], list[str], list[str]]:
    """Return (genres, features, platforms) from a raw tags list."""
    genres, features, platforms = [], [], []
    seen_g, seen_f, seen_p = set(), set(), set()

    for tag in tags or []:
        name  = tag.get("name", "")
        group = (tag.get("groupName") or "").lower()
        if not name or name.isdigit():
            continue
        if "genre" in group and name not in seen_g:
            genres.append(name.title()); seen_g.add(name)
        elif "platform" in group and name not in seen_p:
            platforms.append(name.title()); seen_p.add(name)
        elif ("feature" in group or name.upper().replace(" ", "_") in FEATURE_TAGS) and name not in seen_f:
            features.append(name.title()); seen_f.add(name)

    return genres, features, platforms


def _unique(seq: list) -> list:
    """Deduplicate while preserving order."""
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out


# ---------------------------------------------------------------------------
# Slug helpers
# ---------------------------------------------------------------------------

def _is_dead_slug(slug: str) -> bool:
    return len(slug) == 32 and slug.isalnum()


def _clean_slug(slug: str) -> str:
    slug = slug.replace("/home", "")
    return slug[2:] if slug.startswith("p/") else slug


def get_slugs(item: dict) -> list[str]:
    raw: list[str] = []
    for mapping in item.get("catalogNs", {}).get("mappings", []):
        if s := mapping.get("pageSlug"): raw.append(s)
    for mapping in item.get("offerMappings", []):
        if s := mapping.get("pageSlug"): raw.append(s)
    for key in ("productSlug", "urlSlug"):
        if s := item.get(key): raw.append(s)
    return _unique([_clean_slug(s) for s in raw])


def pick_best_page(pages: list[dict]) -> dict:
    valid = [p for p in pages if p.get("data")]
    if not valid:
        return {}
    for p in valid:
        if p.get("type") == "productHome" or p.get("_slug") == "home":
            return p
    valid.sort(key=lambda p: len(p.get("_slug", p.get("pageName", "x" * 100))))
    return valid[0]


# ---------------------------------------------------------------------------
# Thumbnail mapping
# ---------------------------------------------------------------------------

def get_thumbnail_mapping(page_node: dict) -> dict[str, str]:
    """Map video mediaRefId → best thumbnail URL from the page tree."""
    mapping: dict[str, str] = {}

    def _walk(node, current_image: str = "") -> None:
        if isinstance(node, dict):
            img_data = node.get("image")
            if isinstance(img_data, dict):
                current_image = img_data.get("src", current_image)
            elif isinstance(img_data, str):
                current_image = img_data

            if "recipes" in node:
                recipes_str = node["recipes"]
                if isinstance(recipes_str, str) and "{" in recipes_str:
                    try:
                        data = json.loads(recipes_str)
                        if isinstance(data, dict):
                            for formats in data.values():
                                if not isinstance(formats, list):
                                    continue
                                for fmt in formats:
                                    if not isinstance(fmt, dict):
                                        continue
                                    ref_id = fmt.get("mediaRefId")
                                    if not ref_id:
                                        continue
                                    thumb = next(
                                        (o["url"] for o in fmt.get("outputs", [])
                                         if isinstance(o, dict)
                                         and o.get("key") == "thumbnail"
                                         and o.get("url")),
                                        current_image,
                                    )
                                    if thumb:
                                        mapping[ref_id] = thumb
                    except json.JSONDecodeError:
                        pass
            elif isinstance(node.get("mediaRefId"), str) and current_image:
                mapping[node["mediaRefId"]] = current_image

            for v in node.values():
                if isinstance(v, (dict, list)):
                    _walk(v, current_image)

        elif isinstance(node, list):
            for item in node:
                if isinstance(item, (dict, list)):
                    _walk(item, current_image)

    _walk(page_node)
    return mapping


# ---------------------------------------------------------------------------
# API fetchers
# ---------------------------------------------------------------------------

def _graphql_get(scraper: cloudscraper.CloudScraper, operation: str,
                 variables: dict, hash_key: str, timeout: int = 8) -> dict:
    params = {
        "operationName": operation,
        "variables":     json.dumps(variables),
        "extensions":    json.dumps({"persistedQuery": {"version": 1, "sha256Hash": QUERY_HASHES[hash_key]}}),
    }
    try:
        res = scraper.get(GRAPHQL_URL, params=params, timeout=timeout)
        if res.status_code == 200:
            return res.json().get("data", {})
    except Exception:
        pass
    return {}


def fetch_epic_rating(scraper: cloudscraper.CloudScraper, sandbox_id: str) -> float:
    query = """
    query getProductResult($sandboxId: String!, $locale: String!) {
      RatingsPolls {
        getProductResult(sandboxId: $sandboxId, locale: $locale) { averageRating }
      }
    }
    """
    payload = {
        "operationName": "getProductResult",
        "query": query.strip(),
        "variables": {"sandboxId": sandbox_id, "locale": "en-US"},
    }
    try:
        res = scraper.post(
            GRAPHQL_URL,
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=8,
        )
        if res.status_code == 200:
            rating = (
                res.json()
                .get("data", {})
                .get("RatingsPolls", {})
                .get("getProductResult", {})
                .get("averageRating")
            )
            if rating:
                return round(float(rating), 1)
    except Exception:
        pass
    return 0.0


def fetch_home_config(scraper: cloudscraper.CloudScraper, sandbox_id: str) -> dict:
    data = _graphql_get(
        scraper, "getProductHomeConfig",
        {"locale": "en-US", "sandboxId": sandbox_id},
        "home_config",
    )
    result: dict = {"screenshots": [], "video_data": []}
    for config in data.get("Product", {}).get("sandbox", {}).get("configuration", []):
        for img in config.get("configs", {}).get("keyImages", []):
            url = img.get("url", "")
            if "com.epicgames.video://" in url:
                parts = url.split("com.epicgames.video://")[1].split("?cover=")
                result["video_data"].append({
                    "id":    parts[0],
                    "cover": urllib.parse.unquote(parts[1]) if len(parts) > 1 else "",
                })
            elif img.get("type") == "featuredMedia":
                result["screenshots"].append(url)
    return result


def resolve_video_ref_id(scraper: cloudscraper.CloudScraper, video_item_id: str) -> str | None:
    query = """
    query getVideoById($videoId: String!, $locale: String!) {
      Video { fetchVideoByLocale(videoId: $videoId, locale: $locale) { recipe mediaRefId } }
    }
    """
    payload = {
        "query": query.strip(),
        "variables": {"videoId": video_item_id, "locale": "en-US"},
    }
    try:
        res = scraper.post(GRAPHQL_URL, json=payload, timeout=8)
        if res.status_code == 200:
            for item in (
                res.json().get("data", {}).get("Video", {}).get("fetchVideoByLocale", [])
            ):
                if item.get("mediaRefId"):
                    return item["mediaRefId"]
    except Exception:
        pass
    return None


def fetch_store_config(scraper: cloudscraper.CloudScraper, sandbox_id: str) -> dict | None:
    data = _graphql_get(
        scraper, "getStoreConfig",
        {"locale": "en-US", "sandboxId": sandbox_id},
        "store_config",
    )
    for config in data.get("Product", {}).get("sandbox", {}).get("configuration", []):
        if "configs" in config:
            return config["configs"]
    return None


def fetch_store_details(api: EpicGamesStoreAPI, item: dict) -> dict | None:
    for slug in get_slugs(item):
        try:
            details = api.get_product(slug)
            if details and details.get("pages"):
                return details
        except Exception:
            continue
    return None


def fetch_catalog_offer(scraper: cloudscraper.CloudScraper,
                        namespace: str, catalog_id: str) -> dict | None:
    data = _graphql_get(
        scraper, "getCatalogOffer",
        {"locale": "en-US", "country": "US", "offerId": catalog_id, "sandboxId": namespace},
        "catalog",
    )
    return data.get("Catalog", {}).get("catalogOffer")


def fetch_video_url(scraper: cloudscraper.CloudScraper, ref_id: str) -> dict | None:
    data = _graphql_get(
        scraper, "getVideo",
        {"mediaRefId": ref_id.replace("-", "")},
        "video",
        timeout=5,
    )
    outputs = data.get("Media", {}).get("getMediaRef", {}).get("outputs", [])
    by_key = {o["key"].lower(): o["url"] for o in outputs if o.get("key") and o.get("url")}
    video_url = by_key.get("manifest") or by_key.get("high")
    if video_url:
        return {
            "video":     video_url,
            "thumbnail": by_key.get("image") or by_key.get("thumbnail") or "",
        }
    return None


def fetch_all_trailers(scraper: cloudscraper.CloudScraper,
                       ref_ids: list[str],
                       thumb_map: dict[str, str] | None = None) -> list[dict]:
    thumb_map = thumb_map or {}
    results: dict[str, dict] = {}

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {executor.submit(fetch_video_url, scraper, rid): rid for rid in ref_ids}
        for future in as_completed(future_map):
            rid = future_map[future]
            vid = future.result()
            if not vid:
                continue
            good_thumb = thumb_map.get(rid)
            if good_thumb and (not vid.get("thumbnail") or "-00001-" in vid["thumbnail"]):
                vid["thumbnail"] = good_thumb
            results[rid] = vid

    return [results[rid] for rid in ref_ids if rid in results]


# ---------------------------------------------------------------------------
# Data extraction – primary path
# ---------------------------------------------------------------------------

def extract_metadata(page_node: dict, game_data: dict, offer: dict | None = None) -> dict:
    page_data = page_node.get("data", {})
    meta      = page_data.get("meta", {})
    about     = page_data.get("about", {})
    offer     = offer or {}
    title     = game_data.get("title", "")

    short_desc = about.get("shortDescription") or game_data.get("description", "")
    if short_desc.strip().lower() == title.strip().lower():
        short_desc = ""

    raw_date = (
        meta.get("releaseDate")
        or game_data.get("releaseDate")
        or game_data.get("effectiveDate")
        or ""
    )
    release_year = _parse_date(raw_date)

    developer: list[str] = meta.get("developer") or []
    publisher: list[str] = meta.get("publisher") or []
    if not developer and about.get("developerAttribution"):
        developer = [about["developerAttribution"]]
    if not publisher and about.get("publisherAttribution"):
        publisher = [about["publisherAttribution"]]

    if not developer or not publisher:
        for attr in game_data.get("customAttributes", []):
            key, val = attr.get("key", "").lower(), attr.get("value", "")
            if not developer and "developer" in key:
                developer = [v.strip() for v in val.split(",")]
            if not publisher and "publisher" in key:
                publisher = [v.strip() for v in val.split(",")]

    tags_source          = offer.get("tags") or game_data.get("tags") or []
    genres, features, _  = _extract_tags(tags_source)

    return {
        "release_year":      release_year,
        "developer":         _unique(developer),
        "publisher":         _unique(publisher),
        "platform":          meta.get("platform") or [],
        "genres":            genres,
        "features":          features,
        "description":       about.get("description", "") or game_data.get("description", ""),
        "short_description": short_desc,
    }


def extract_requirements(store_details: dict) -> dict:
    pages = store_details.get("pages", [])
    reqs  = pick_best_page(pages).get("data", {}).get("requirements", {})
    if not reqs:
        for page in pages:
            if r := page.get("data", {}).get("requirements", {}):
                reqs = r
                break

    systems: dict[str, dict] = {}
    for system in reqs.get("systems", []):
        sys_type    = system.get("systemType", "Unknown")
        minimum: dict     = {}
        recommended: dict = {}
        for detail in system.get("details", []):
            if title := detail.get("title"):
                minimum[title]     = detail.get("minimum", "")
                recommended[title] = detail.get("recommended", "")
        systems[sys_type] = {"minimum": minimum, "recommended": recommended}

    return {"languages": reqs.get("languages", []), "systems": systems}


def extract_ratings(store_details: dict) -> list[dict]:
    pages       = store_details.get("pages", [])
    raw_ratings = pick_best_page(pages).get("productRatings", {}).get("ratings", [])
    if not raw_ratings:
        for page in pages:
            if r := page.get("productRatings", {}).get("ratings", []):
                raw_ratings = r
                break

    seen:   set[str]   = set()
    result: list[dict] = []
    for r in raw_ratings:
        if (title := r.get("title")) and title not in seen:
            seen.add(title)
            result.append({
                "title":         title,
                "country_codes": r.get("countryCodes", ""),
                "image":         r.get("image", {}).get("src", ""),
            })
    return result


def collect_screenshots(page_node: dict, game_data: dict, exclude: set[str]) -> list[str]:
    seen    = set(exclude)
    results: list[str] = []

    def _add(url: str) -> None:
        if url and url not in seen and not _is_bad_image(url):
            seen.add(url)
            results.append(url)

    for item in page_node.get("data", {}).get("carousel", {}).get("items", []):
        _add(item.get("image", {}).get("src", ""))
    for url in page_node.get("_images_", []):
        _add(url)
    for img in game_data.get("keyImages", []):
        if img.get("type") in VALID_IMAGE_TYPES:
            _add(img.get("url", ""))

    return results[:10]


# ---------------------------------------------------------------------------
# Trailer ref-ID extraction
# ---------------------------------------------------------------------------

def _parse_recipes(recipes) -> str | None:
    if isinstance(recipes, str) and "{" in recipes:
        try:
            recipes = json.loads(recipes)
        except json.JSONDecodeError:
            return None
    if not isinstance(recipes, dict):
        return None

    en_keys    = [k for k in recipes if k.lower().startswith("en")]
    other_keys = [k for k in recipes if not k.lower().startswith("en")]

    for k in en_keys + other_keys:
        formats = recipes.get(k, [])
        if not isinstance(formats, list):
            continue
        ref_by_recipe = {
            fmt["recipe"]: fmt["mediaRefId"]
            for fmt in formats
            if isinstance(fmt, dict) and fmt.get("recipe") and fmt.get("mediaRefId")
        }
        for recipe_type in ("video-fmp4", "video-hls", "video-webm"):
            if recipe_type in ref_by_recipe:
                return ref_by_recipe[recipe_type]
    return None


def _walk_recipes(node, ref_ids: list[str]) -> None:
    if isinstance(node, dict):
        if "recipes" in node:
            if (ref_id := _parse_recipes(node["recipes"])) and ref_id not in ref_ids:
                ref_ids.append(ref_id)
        ref_id = node.get("mediaRefId")
        if isinstance(ref_id, str) and len(ref_id) > 10 and ref_id not in ref_ids:
            ref_ids.append(ref_id)
        for v in node.values():
            _walk_recipes(v, ref_ids)
    elif isinstance(node, list):
        for item in node:
            _walk_recipes(item, ref_ids)


def extract_page_ref_ids(page_node: dict) -> list[str]:
    ref_ids: list[str] = []
    _walk_recipes(page_node, ref_ids)
    return ref_ids


# ---------------------------------------------------------------------------
# Fallback path – build result from raw catalog data
# ---------------------------------------------------------------------------

def build_result_from_catalog(
    offer:                dict,
    game_data:            dict,
    store_config:         dict | None  = None,
    trailers:             list  | None = None,
    extracted_screenshots: list | None = None,
) -> dict:
    offer        = offer        or {}
    store_config = store_config or {}

    raw_date = (
        offer.get("releaseDate")
        or offer.get("effectiveDate")
        or game_data.get("releaseDate")
        or game_data.get("effectiveDate")
        or store_config.get("pcReleaseDate", "")
    )
    release_year = _parse_date(raw_date)

    developer: list[str] = []
    publisher: list[str] = []
    if store_config.get("developerDisplayName"):
        developer = [store_config["developerDisplayName"]]
    if store_config.get("publisherDisplayName"):
        publisher = [store_config["publisherDisplayName"]]
    if not developer or not publisher:
        for attrs in (offer.get("customAttributes") or [], game_data.get("customAttributes") or []):
            for attr in attrs:
                key, val = attr.get("key", "").lower(), attr.get("value", "")
                if not developer and "developer" in key:
                    developer = [v.strip() for v in val.split(",") if v.strip()]
                if not publisher and "publisher" in key:
                    publisher = [v.strip() for v in val.split(",") if v.strip()]
            if developer and publisher:
                break

    tags_source = (
        store_config.get("tags") or offer.get("tags") or game_data.get("tags") or []
    )
    genres, features, platform = _extract_tags(tags_source)

    # Requirements
    requirements: dict = {"languages": [], "systems": {}}
    if tech := store_config.get("technicalRequirements", {}):
        for os_name in ("windows", "macos", "linux"):
            if reqs_list := tech.get(os_name):
                sys_dict: dict = {"minimum": {}, "recommended": {}}
                for req in reqs_list:
                    if title := req.get("title"):
                        sys_dict["minimum"][title]     = req.get("minimum", "")
                        sys_dict["recommended"][title] = req.get("recommended", "")
                requirements["systems"][OS_DISPLAY.get(os_name, os_name.capitalize())] = sys_dict

    langs: list[str] = []
    if audio := store_config.get("supportedAudio"):
        langs.append("AUDIO: " + ", ".join(audio))
    if text := store_config.get("supportedText"):
        langs.append("TEXT: " + ", ".join(text))
    requirements["languages"] = langs

    # Platform fallback
    if not platform:
        platform = [
            OS_DISPLAY.get(k.lower(), k)
            for k in requirements.get("systems", {})
        ]
    if not platform:
        for img in (offer.get("keyImages") or game_data.get("keyImages") or []):
            url = (img.get("url") or "").lower()
            if "win" in url and "Windows" not in platform:
                platform.append("Windows")
            elif "mac" in url and "Mac" not in platform:
                platform.append("Mac")

    key_images  = offer.get("keyImages") or game_data.get("keyImages") or []
    poster_url  = next((i["url"] for i in key_images if i and i.get("type") in TALL_IMAGE_TYPES), "")
    bg_url      = next((i["url"] for i in key_images if i and i.get("type") in WIDE_IMAGE_TYPES), "")
    logo_url    = next(
        (i.get("url", "") for i in (store_config.get("keyImages") or [])
         if i and i.get("type") == "ProductLogo"),
        "",
    )

    screenshots = extracted_screenshots or [
        img["url"]
        for img in key_images
        if img and img.get("type") in VALID_IMAGE_TYPES and not _is_bad_image(img.get("url", ""))
    ][:10]

    banner      = store_config.get("banner") or {}
    description = (
        offer.get("longDescription")
        or game_data.get("longDescription")
        or banner.get("description")
        or offer.get("description")
        or store_config.get("description")
        or game_data.get("description", "")
    )

    return {
        "title":             store_config.get("productDisplayName") or offer.get("title") or game_data.get("title", ""),
        "short_description": offer.get("description") or game_data.get("description", ""),
        "description":       description,
        "avg_rating":        offer.get("averageRating") or game_data.get("averageRating", 0),
        "release_year":      release_year,
        "developer":         _unique(developer),
        "publisher":         _unique(publisher),
        "platform":          _unique(platform),
        "genres":            genres,
        "features":          features,
        "ratings":           [],
        "requirements":      requirements,
        "poster":            poster_url,
        "background":        bg_url,
        "logo":              logo_url,
        "trailers":          trailers or [],
        "screenshots":       screenshots[:10],
    }


# ---------------------------------------------------------------------------
# Enrichment – fill Demo/Trial gaps from the base game
# ---------------------------------------------------------------------------

def enrich_from_base_game(
    current_result:     dict,
    is_enriching:       bool,
    original_namespace: str,
) -> dict:
    if is_enriching:
        return current_result

    title       = current_result.get("title", "")
    clean_title = re.sub(r"(?i)\b(demo|free trial|trial)\b", "", title)
    clean_title = clean_title.replace("®", "").replace("™", "").strip()

    is_trial = clean_title.lower() != title.lower().replace("®", "").replace("™", "").strip()

    if not is_trial and current_result.get("developer") and current_result.get("screenshots"):
        return current_result

    if not clean_title or not is_trial:
        return current_result

    log.info("Fetching base game '%s' (namespace: %s) to enrich demo…", clean_title, original_namespace)
    base_data = get_baddel_data(clean_title, is_enriching=True, target_namespace=original_namespace)
    if not base_data:
        return current_result

    if base_data.get("description"):
        current_result["description"] = base_data["description"]
    if base_data.get("short_description"):
        current_result["short_description"] = base_data["short_description"]

    for key in ("developer", "publisher", "genres", "features", "platform",
                "trailers", "screenshots", "logo"):
        if not current_result.get(key) and base_data.get(key):
            current_result[key] = base_data[key]

    if (
        not current_result.get("requirements", {}).get("systems")
        and base_data.get("requirements", {}).get("systems")
    ):
        current_result["requirements"] = base_data["requirements"]

    return current_result


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def get_baddel_data(
    game_title:       str,
    is_enriching:     bool = False,
    target_namespace: str  = "",
) -> dict | None:
    api     = EpicGamesStoreAPI()
    scraper = cloudscraper.create_scraper()

    search   = api.fetch_store_games(keywords=game_title, count=20)
    elements = search.get("data", {}).get("Catalog", {}).get("searchStore", {}).get("elements", [])
    if not elements:
        return None

    target_clean = game_title.lower().replace("®", "").replace("™", "").strip()

    def match_score(item: dict) -> int:
        if target_namespace and item.get("namespace") == target_namespace:
            return -10
        item_title = item.get("title", "").lower().replace("®", "").replace("™", "").strip()
        if item_title == target_clean:
            return 0
        if target_clean in item_title:
            return 1
        return 2

    elements.sort(key=lambda x: (
        match_score(x),
        x.get("offerType") != "BASE_GAME",
        x.get("offerType") == "ADD_ON",
    ))

    # ── PRIMARY PATH: store pages available ──────────────────────────────────
    game_data, store_details = None, None
    for item in elements:
        if details := fetch_store_details(api, item):
            game_data, store_details = item, details
            break

    if store_details:
        page_node  = pick_best_page(store_details["pages"])
        namespace  = game_data.get("namespace", "")
        catalog_id = game_data.get("id", "")

        # Parallelise independent I/O calls
        with ThreadPoolExecutor(max_workers=3) as ex:
            f_offer  = ex.submit(fetch_catalog_offer, scraper, namespace, catalog_id) if (namespace and catalog_id) else None
            f_rating = ex.submit(fetch_epic_rating, scraper, namespace) if namespace else None

        offer       = (f_offer.result()  if f_offer  else None) or {}
        real_rating = (f_rating.result() if f_rating else 0.0)
        avg_rating  = real_rating if real_rating > 0 else game_data.get("averageRating", 0)

        hero           = page_node.get("data", {}).get("hero", {})
        logo_url       = hero.get("logoImage", {}).get("src", "")
        background_url = hero.get("backgroundImageUrl", "")
        poster_url     = next(
            (img["url"] for img in game_data.get("keyImages", [])
             if img.get("type") in TALL_IMAGE_TYPES),
            "",
        )

        exclude_set  = {u for u in (logo_url, poster_url, background_url) if u}
        screenshots  = collect_screenshots(page_node, game_data, exclude_set)
        meta         = extract_metadata(page_node, game_data, offer)
        requirements = extract_requirements(store_details)
        ratings      = extract_ratings(store_details)

        direct_trailers = list(dict.fromkeys(
            re.findall(r'https?://[^\s"\'\{\}\[\]]+\.mp4', json.dumps(page_node))
        ))
        if direct_trailers:
            trailers = direct_trailers
        else:
            ref_ids   = extract_page_ref_ids(page_node)
            thumb_map = get_thumbnail_mapping(page_node)
            trailers  = fetch_all_trailers(scraper, ref_ids, thumb_map)

        result = {
            "title":             game_data.get("title"),
            "short_description": meta.get("short_description", ""),
            "description":       meta["description"],
            "avg_rating":        avg_rating,
            "release_year":      meta["release_year"],
            "developer":         meta["developer"],
            "publisher":         meta["publisher"],
            "platform":          meta["platform"],
            "genres":            meta["genres"],
            "features":          meta["features"],
            "ratings":           ratings,
            "requirements":      requirements,
            "poster":            poster_url,
            "background":        background_url,
            "logo":              logo_url,
            "trailers":          trailers,
            "screenshots":       screenshots[:10],
        }
        return enrich_from_base_game(result, is_enriching, namespace)

    # ── FALLBACK PATH: build from raw catalog data ───────────────────────────
    best_result, best_score, best_namespace = None, -1, ""

    for item in elements:
        namespace  = item.get("namespace", "")
        catalog_id = item.get("id", "")

        if target_namespace and namespace != target_namespace:
            continue

        # Parallelise independent I/O calls per item
        with ThreadPoolExecutor(max_workers=4) as ex:
            f_offer   = ex.submit(fetch_catalog_offer, scraper, namespace, catalog_id) if (namespace and catalog_id) else None
            f_rating  = ex.submit(fetch_epic_rating,   scraper, namespace)
            f_config  = ex.submit(fetch_store_config,  scraper, namespace)
            f_home    = ex.submit(fetch_home_config,   scraper, namespace)

        offer        = (f_offer.result()  if f_offer  else None) or {}
        real_rating  = f_rating.result()
        store_config = f_config.result()
        home_data    = f_home.result()

        screenshots: list[str]  = home_data.get("screenshots", [])
        trailers:    list[dict] = []

        for v_item in home_data.get("video_data", []):
            if ref_id := resolve_video_ref_id(scraper, v_item["id"]):
                if vid := fetch_video_url(scraper, ref_id):
                    if not vid.get("thumbnail") or "-00001-" in vid["thumbnail"]:
                        vid["thumbnail"] = v_item["cover"]
                    trailers.append(vid)

        result              = build_result_from_catalog(offer, item, store_config, trailers, screenshots)
        result["avg_rating"] = real_rating

        desc       = result.get("description", "")
        has_ss     = bool(result.get("screenshots"))
        has_desc   = desc and len(desc) > 40 and desc.strip().lower() != result.get("title", "").strip().lower()
        score      = len(result.get("screenshots", [])) * 100 + len(desc)

        if score > best_score:
            best_result, best_score, best_namespace = result, score, namespace

        if has_ss or has_desc:
            return enrich_from_base_game(result, is_enriching, namespace)

    if best_result:
        return enrich_from_base_game(best_result, is_enriching, best_namespace)

    return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    target = " ".join(sys.argv[1:]) or "Control"

    start  = time.perf_counter()
    result = get_baddel_data(target)
    elapsed = time.perf_counter() - start

    if result:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        log.warning("No data found for '%s'.", target)

    log.info("Time taken: %.2f seconds", elapsed)