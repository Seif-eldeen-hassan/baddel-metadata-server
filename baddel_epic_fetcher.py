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

REQ_KEY_MAP = {
    "os version": "os_versions",
    "os versions": "os_versions",
    "operating system": "os_versions",
    "os": "os_versions",
    "cpu": "cpu",
    "processor": "cpu",
    "memory": "ram",
    "ram": "ram",
    "gpu": "gpu",
    "graphics": "gpu",
    "video card": "gpu",
    "storage": "storage",
    "hard drive": "storage",
    "hard disk space": "storage",
    "disk space": "storage",
    "directx": "directx",
    "direct x": "directx",
    "directx version": "directx",
    "direct x version": "directx",
    "additional notes": "notes",
    "notes": "notes",
    "sound card": "notes",
    "network": "notes",
}


def _std_req_key(title: str) -> str:
    key = re.sub(r"\s+", " ", str(title or "").lower().strip().rstrip(":"))
    return REQ_KEY_MAP.get(key, key.replace(" ", "_"))

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


def _norm_text(value: str) -> str:
    """Normalize names for safe matching."""
    value = str(value or "").lower().replace("®", "").replace("™", "").replace("©", "")
    value = re.sub(r"&", " and ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _slugify(value: str) -> str:
    return _norm_text(value).replace(" ", "-")


def _title_from_slug(slug: str) -> str:
    value = str(slug or "").strip().strip("/").split("?")[0]
    value = value.replace("/home", "")
    if value.startswith("p/"):
        value = value[2:]
    value = value.split("/")[-1]
    value = value.replace("-", " ").replace("_", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value.title()


def _token_jaccard(a: str, b: str) -> float:
    sa = set(_norm_text(a).split())
    sb = set(_norm_text(b).split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _looks_like_namespace(value: str) -> bool:
    value = str(value or "").strip()
    if not value:
        return False
    if len(value) == 32 and re.fullmatch(r"[a-f0-9]{32}", value, re.IGNORECASE):
        return True
    # Epic has many short internal codenames such as rose/calluna/petunia.
    return bool(re.fullmatch(r"[a-z][a-z0-9_-]{2,40}", value, re.IGNORECASE)) and " " not in value


def _find_first_key(obj, keys: set[str], max_depth: int = 5):
    if max_depth < 0:
        return None
    if isinstance(obj, dict):
        for key, value in obj.items():
            if str(key).lower() in keys and value not in (None, "", []):
                return value
        for value in obj.values():
            found = _find_first_key(value, keys, max_depth - 1)
            if found not in (None, "", []):
                return found
    elif isinstance(obj, list):
        for item in obj[:50]:
            found = _find_first_key(item, keys, max_depth - 1)
            if found not in (None, "", []):
                return found
    return None


def _collect_key_images(obj, max_depth: int = 5) -> list[dict]:
    out: list[dict] = []

    def walk(node, depth: int) -> None:
        if depth < 0:
            return
        if isinstance(node, dict):
            if isinstance(node.get("keyImages"), list):
                for image in node["keyImages"]:
                    if isinstance(image, dict) and image.get("url"):
                        out.append(image)
            for value in node.values():
                if isinstance(value, (dict, list)):
                    walk(value, depth - 1)
        elif isinstance(node, list):
            for item in node[:100]:
                if isinstance(item, (dict, list)):
                    walk(item, depth - 1)

    walk(obj, max_depth)
    return _unique([json.dumps(i, sort_keys=True) for i in out]) and [json.loads(s) for s in _unique([json.dumps(i, sort_keys=True) for i in out])]


def _image_url(candidate: dict) -> str:
    if not isinstance(candidate, dict):
        return ""
    for key in ("url", "src", "href"):
        value = candidate.get(key)
        if isinstance(value, str) and value.startswith("http"):
            return value
    image = candidate.get("image")
    if isinstance(image, dict):
        value = image.get("url") or image.get("src")
        if isinstance(value, str) and value.startswith("http"):
            return value
    return ""


def _image_type(candidate: dict) -> str:
    if not isinstance(candidate, dict):
        return ""
    for key in ("type", "imageType", "key", "name", "usage", "role"):
        value = candidate.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _collect_image_candidates(*objects, max_depth: int = 7) -> list[dict]:
    """Collect all image-like URLs from Epic payloads, not only keyImages."""
    out: list[dict] = []

    def add(url: str, image_type: str = "", source_key: str = "") -> None:
        if not isinstance(url, str) or not url.startswith("http"):
            return
        lower = url.lower()
        if not any(x in lower for x in (".jpg", ".jpeg", ".png", ".webp", "unrealengine.com", "epicgames.com")):
            return
        out.append({"url": url, "type": image_type or "", "source_key": source_key or ""})

    def walk(node, depth: int = max_depth, context: str = "") -> None:
        if depth < 0:
            return
        if isinstance(node, dict):
            node_type = _image_type(node) or context
            url = _image_url(node)
            if url:
                add(url, node_type, context)

            for key in ("image", "logoImage", "backgroundImage", "thumbnail", "media", "portrait", "landscape"):
                value = node.get(key)
                if isinstance(value, dict):
                    nested_url = _image_url(value)
                    if nested_url:
                        add(nested_url, _image_type(value) or key, key)
                elif isinstance(value, str) and value.startswith("http"):
                    add(value, key, key)

            for key, value in node.items():
                key_l = str(key).lower()
                if isinstance(value, str) and value.startswith("http"):
                    add(value, node_type or key_l, key_l)
                elif isinstance(value, (dict, list)):
                    walk(value, depth - 1, node_type or key_l)
        elif isinstance(node, list):
            for item in node[:250]:
                if isinstance(item, (dict, list)):
                    walk(item, depth - 1, context)

    for obj in objects:
        walk(obj)

    seen: set[str] = set()
    deduped: list[dict] = []
    for item in out:
        url = item.get("url", "")
        if url and url not in seen:
            seen.add(url)
            deduped.append(item)
    return deduped


def _url_dimensions(url: str) -> tuple[int, int] | None:
    matches = re.findall(r"(?<!\d)(\d{2,5})x(\d{2,5})(?!\d)", url or "")
    if not matches:
        return None
    pairs = [(int(w), int(h)) for w, h in matches]
    return max(pairs, key=lambda wh: wh[0] * wh[1])


def _asset_text(value: str) -> str:
    """Lowercase and unquote URL/text so asset matching sees path segments."""
    return urllib.parse.unquote(str(value or "")).lower()


def _asset_terms(*values: str) -> set[str]:
    """Build title/slug terms that should appear in correct artwork URLs."""
    terms: set[str] = set()
    stop = {
        "the", "and", "of", "a", "an", "edition", "definitive", "standard",
        "game", "games", "store", "epic", "egs", "pc", "windows", "mac",
    }
    for value in values:
        raw = urllib.parse.unquote(str(value or "")).lower()
        if not raw:
            continue
        compact = re.sub(r"[^a-z0-9]+", "", raw)
        if len(compact) >= 4:
            terms.add(compact)
        for token in re.split(r"[^a-z0-9]+", raw):
            if len(token) >= 4 and token not in stop:
                terms.add(token)
    return terms


def _candidate_matches_terms(url: str, terms: set[str]) -> bool:
    if not terms:
        return True
    text = re.sub(r"[^a-z0-9]+", "", _asset_text(url))
    spaced = _asset_text(url)
    return any(term in text or term in spaced for term in terms)


def _non_base_asset_penalty(url: str, preferred_terms: set[str]) -> int:
    """Penalize DLC/expansion artwork when enriching the base game."""
    if not preferred_terms:
        return 0
    if _candidate_matches_terms(url, preferred_terms):
        return 0
    text = _asset_text(url)
    hints = (
        "dlc", "add-on", "addon", "expansion", "season-pass", "seasonpass",
        "the-foundation", "foundation", "awe", "episode", "chapter", "pack",
    )
    return -1800 if any(h in text for h in hints) else 0


def _poster_score(candidate: dict, preferred_terms: set[str] | None = None) -> int:
    url = candidate.get("url", "")
    if not url:
        return -9999

    lower_url = url.lower()
    type_l = str(candidate.get("type", "")).lower()
    source_l = str(candidate.get("source_key", "")).lower()
    text = f"{type_l} {source_l} {lower_url}"

    if any(x in text for x in ("logo", "icon", "rating", "pegi", "esrb", "usk", "cero")):
        return -9999
    if lower_url.endswith(".svg"):
        return -9999

    score = 0
    preferred_terms = preferred_terms or set()

    # Title/slug relevance beats generic tall-image hints. This prevents DLC
    # capsules (e.g. Control: AWE/Foundation) from becoming the base cover.
    if preferred_terms and not _candidate_matches_terms(url, preferred_terms):
        score -= 2400
    score += _non_base_asset_penalty(url, preferred_terms)

    if type_l in {t.lower() for t in TALL_IMAGE_TYPES}:
        score += 1000
    if any(x in text for x in ("offerimagetall", "dieselstorefronttall", "storefronttall", "portrait", "vertical", "boxart", "cover", "poster")):
        score += 850

    # Epic tall capsule images are commonly named S2 and sized 1200x1600 / 860x1148.
    if re.search(r"(^|[_\-/])s2([_\-.]|$)", lower_url) or "_s2" in lower_url or "-s2" in lower_url:
        score += 650

    dims = _url_dimensions(url)
    if dims:
        w, h = dims
        if h > w:
            score += 500 + min(250, int((h / max(w, 1) - 1) * 180))
        elif w > h:
            score -= 250
        if w < 250 or h < 250:
            score -= 600
        if (w, h) in {(1200, 1600), (860, 1148), (600, 800), (300, 400)}:
            score += 350

    if any(x in text for x in ("background", "landscape", "wide", "screenshot", "gallery", "carousel", "featuredmedia")):
        score -= 350

    return score


def pick_best_poster_url(*objects, fallback_url: str = "", preferred_terms: set[str] | None = None) -> str:
    candidates = _collect_image_candidates(*objects)
    preferred_terms = preferred_terms or set()
    scored = [(item, _poster_score(item, preferred_terms)) for item in candidates]
    scored = [(item, score) for item, score in scored if score > -9999]
    if not scored:
        return fallback_url or ""

    scored.sort(key=lambda pair: pair[1], reverse=True)
    best, score = scored[0]
    if score > 0:
        return best.get("url", "")

    # Last-resort fallback so the DB doesn't receive an empty poster.
    return fallback_url or best.get("url", "") or ""


def pick_best_wide_url(*objects, fallback_url: str = "", preferred_terms: set[str] | None = None) -> str:
    candidates = _collect_image_candidates(*objects)
    preferred_terms = preferred_terms or set()
    best_url = fallback_url or ""
    best_score = 0
    for item in candidates:
        url = item.get("url", "")
        if not url:
            continue
        lower = url.lower()
        type_l = str(item.get("type", "")).lower()
        if any(x in lower for x in ("logo", "icon", "rating", "pegi", "esrb", "usk", "cero")):
            continue
        score = 0
        if preferred_terms and not _candidate_matches_terms(url, preferred_terms):
            score -= 1600
        score += _non_base_asset_penalty(url, preferred_terms)
        if type_l in {t.lower() for t in WIDE_IMAGE_TYPES}:
            score += 700
        if any(x in f"{type_l} {lower}" for x in ("background", "landscape", "wide", "hero")):
            score += 500
        dims = _url_dimensions(url)
        if dims:
            w, h = dims
            if w > h:
                score += 250
            else:
                score -= 150
        if score > best_score:
            best_score = score
            best_url = url
    return best_url


def _extract_pages(product: dict) -> list[dict]:
    if not isinstance(product, dict):
        return []
    if isinstance(product.get("pages"), list):
        return product["pages"]
    pages = _find_first_key(product, {"pages"}, max_depth=4)
    return pages if isinstance(pages, list) else []


def _product_title(product: dict, fallback: str = "") -> str:
    title = _find_first_key(product, {"title", "name", "productname", "productdisplayname"}, max_depth=4)
    if isinstance(title, str) and title.strip():
        return title.strip()
    pages = _extract_pages(product)
    page = pick_best_page(pages) if pages else {}
    meta = page.get("data", {}).get("meta", {})
    about = page.get("data", {}).get("about", {})
    title = meta.get("title") or about.get("title") or page.get("title")
    if isinstance(title, str) and title.strip():
        return title.strip()
    return fallback


def _product_namespace(product: dict) -> str:
    value = _find_first_key(product, {"namespace", "sandboxid", "sandbox_id"}, max_depth=5)
    return str(value or "").strip()


def _product_catalog_id(product: dict) -> str:
    value = _find_first_key(product, {"offerid", "offer_id", "catalogid", "catalog_id", "id"}, max_depth=4)
    return str(value or "").strip()


def _product_to_game_data(product: dict, requested_slug: str = "") -> dict:
    pages = _extract_pages(product)
    page = pick_best_page(pages) if pages else {}
    page_data = page.get("data", {}) if isinstance(page, dict) else {}
    meta = page_data.get("meta", {}) if isinstance(page_data, dict) else {}
    about = page_data.get("about", {}) if isinstance(page_data, dict) else {}

    title = _product_title(product, _title_from_slug(requested_slug))
    description = (
        about.get("shortDescription")
        or about.get("description")
        or meta.get("description")
        or _find_first_key(product, {"description", "shortdescription"}, max_depth=4)
        or ""
    )

    namespace = _product_namespace(product)
    catalog_id = _product_catalog_id(product)
    key_images = _collect_key_images(product)

    return {
        "title": title,
        "description": description,
        "namespace": namespace,
        "id": catalog_id,
        "keyImages": key_images,
        "tags": _find_first_key(product, {"tags"}, max_depth=4) or [],
        "customAttributes": _find_first_key(product, {"customattributes"}, max_depth=4) or [],
        "releaseDate": _find_first_key(product, {"releasedate", "effectivedate"}, max_depth=4) or "",
        "effectiveDate": _find_first_key(product, {"effectivedate"}, max_depth=4) or "",
        "averageRating": _find_first_key(product, {"averagerating"}, max_depth=4) or 0,
    }


def _candidate_slugs_for_target(api: EpicGamesStoreAPI, target: str) -> list[str]:
    target = str(target or "").strip()
    if not target:
        return []

    candidates = [target, _slugify(target)]

    # If target is an Epic namespace/codename, map it to the public product slug.
    # This fixes cases such as namespace 'rose' being mapped to slug 'kine'.
    try:
        mapping = api.get_product_mapping()
        if isinstance(mapping, dict):
            mapped_slug = mapping.get(target)
            if mapped_slug:
                candidates.insert(0, str(mapped_slug))
            target_norm = _norm_text(target)
            target_slug = _slugify(target)
            for ns, slug in mapping.items():
                slug_s = str(slug or "").strip()
                if not slug_s:
                    continue
                if _slugify(slug_s) == target_slug or _norm_text(_title_from_slug(slug_s)) == target_norm:
                    candidates.insert(0, slug_s)
                    break
    except Exception as exc:
        log.debug("Epic product mapping unavailable: %s", exc)

    cleaned = []
    for slug in candidates:
        slug = _clean_slug(str(slug or "").strip())
        if slug and slug not in cleaned and not _is_dead_slug(slug):
            cleaned.append(slug)
    return cleaned


def _is_confident_title_match(query: str, title: str, slugs: list[str] | None = None, namespace: str = "", target_namespace: str = "") -> bool:
    query_norm = _norm_text(query)
    title_norm = _norm_text(title)
    query_slug = _slugify(query)
    slugs = slugs or []

    if not query_norm or not title_norm:
        return False
    if target_namespace and namespace and namespace == target_namespace:
        return True
    if title_norm == query_norm:
        return True
    if query_slug and any(_slugify(s) == query_slug for s in slugs):
        return True

    # Avoid accepting random search results for short titles like "Kine".
    if len(query_norm) <= 5:
        return False

    if query_norm in title_norm or title_norm in query_norm:
        return True
    return _token_jaccard(query_norm, title_norm) >= 0.75


def _match_score_for_item(item: dict, query: str, target_namespace: str = "") -> int:
    namespace = item.get("namespace", "")
    slugs = get_slugs(item)
    title = item.get("title", "")

    if target_namespace and namespace == target_namespace:
        return -20
    if _is_confident_title_match(query, title, slugs, namespace, target_namespace):
        return 0 if _norm_text(title) == _norm_text(query) else 10
    return 999


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
                std_key = _std_req_key(title)
                minimum[std_key]     = detail.get("minimum", "")
                recommended[std_key] = detail.get("recommended", "")
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
                        std_key = _std_req_key(title)
                        sys_dict["minimum"][std_key]     = req.get("minimum", "")
                        sys_dict["recommended"][std_key] = req.get("recommended", "")
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

    title_for_assets = store_config.get("productDisplayName") or offer.get("title") or game_data.get("title", "")
    preferred_terms = _asset_terms(title_for_assets, *(get_slugs(game_data) or []))

    key_images = _collect_key_images({
        "offer": offer,
        "game_data": game_data,
        "store_config": store_config,
    }) or (offer.get("keyImages") or game_data.get("keyImages") or [])

    logo_url = next(
        (i.get("url", "") for i in (store_config.get("keyImages") or [])
         if i and i.get("type") == "ProductLogo"),
        "",
    )

    bg_url = pick_best_wide_url(
        {"keyImages": key_images},
        offer,
        game_data,
        store_config,
        fallback_url=next((i.get("url", "") for i in key_images if i and i.get("type") in WIDE_IMAGE_TYPES), ""),
        preferred_terms=preferred_terms,
    )

    screenshots = extracted_screenshots or [
        img["url"]
        for img in key_images
        if img and img.get("type") in VALID_IMAGE_TYPES and not _is_bad_image(img.get("url", ""))
    ][:10]

    poster_fallback = ""
    if screenshots:
        poster_fallback = screenshots[0]
    elif bg_url:
        poster_fallback = bg_url

    poster_url = pick_best_poster_url(
        {"keyImages": key_images},
        offer,
        game_data,
        store_config,
        {"screenshots": screenshots},
        fallback_url=poster_fallback,
        preferred_terms=preferred_terms,
    )

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




def build_result_from_store_details(
    api: EpicGamesStoreAPI,
    scraper: cloudscraper.CloudScraper,
    store_details: dict,
    game_data: dict,
) -> dict | None:
    """Build the normal Baddel result from Epic product pages + catalog data."""
    pages = store_details.get("pages", [])
    if not pages:
        return None

    page_node  = pick_best_page(pages)
    namespace  = game_data.get("namespace", "")
    catalog_id = game_data.get("id", "")

    with ThreadPoolExecutor(max_workers=4) as ex:
        f_offer  = ex.submit(fetch_catalog_offer, scraper, namespace, catalog_id) if (namespace and catalog_id) else None
        f_rating = ex.submit(fetch_epic_rating, scraper, namespace) if namespace else None
        f_config = ex.submit(fetch_store_config, scraper, namespace) if namespace else None

    offer        = (f_offer.result()  if f_offer  else None) or {}
    real_rating  = (f_rating.result() if f_rating else 0.0)
    store_config = (f_config.result() if f_config else None) or {}
    avg_rating   = real_rating if real_rating > 0 else game_data.get("averageRating", 0)

    hero = page_node.get("data", {}).get("hero", {})

    logo_url = (
        hero.get("logoImage", {}).get("src", "")
        or next(
            (i.get("url", "") for i in _collect_key_images(store_config)
             if i and i.get("type") == "ProductLogo"),
            "",
        )
    )

    title_for_assets = game_data.get("title", "")
    preferred_terms = _asset_terms(title_for_assets, *(get_slugs(game_data) or []))

    raw_background_url = hero.get("backgroundImageUrl", "")
    # The product-home hero is the safest base-game background. Do not override
    # it by scanning offer images, because Epic can include DLC art in the same
    # namespace payload.
    background_url = raw_background_url or pick_best_wide_url(
        game_data,
        page_node,
        offer,
        store_config,
        fallback_url="",
        preferred_terms=preferred_terms,
    )

    exclude_set = {u for u in (logo_url, background_url) if u}
    screenshots = collect_screenshots(page_node, game_data, exclude_set)

    poster_fallback = ""
    if screenshots:
        poster_fallback = screenshots[0]
    elif background_url:
        poster_fallback = background_url

    poster_url = pick_best_poster_url(
        game_data,
        page_node,
        offer,
        store_config,
        {"screenshots": screenshots},
        fallback_url=poster_fallback,
        preferred_terms=preferred_terms,
    )

    exclude_set = {u for u in (logo_url, poster_url, background_url) if u}
    screenshots = collect_screenshots(page_node, game_data, exclude_set)
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

    return {
        "title":             game_data.get("title") or meta.get("title", ""),
        "short_description": meta.get("short_description", ""),
        "description":       meta.get("description", ""),
        "avg_rating":        avg_rating,
        "release_year":      meta.get("release_year", ""),
        "developer":         meta.get("developer", []),
        "publisher":         meta.get("publisher", []),
        "platform":          meta.get("platform", []),
        "genres":            meta.get("genres", []),
        "features":          meta.get("features", []),
        "ratings":           ratings,
        "requirements":      requirements,
        "poster":            poster_url,
        "background":        background_url,
        "logo":              logo_url,
        "trailers":          trailers,
        "screenshots":       screenshots[:10],
    }


def fetch_direct_product_result(api: EpicGamesStoreAPI, scraper: cloudscraper.CloudScraper, target: str) -> tuple[dict | None, str]:
    """Try direct Epic slug / namespace mapping before fuzzy store search."""
    for slug in _candidate_slugs_for_target(api, target):
        try:
            product = api.get_product(slug)
        except Exception as exc:
            log.debug("Direct Epic product fetch failed for slug '%s': %s", slug, exc)
            continue

        if not isinstance(product, dict) or not _extract_pages(product):
            continue

        game_data = _product_to_game_data(product, requested_slug=slug)
        title = game_data.get("title", "")

        # Direct slug match is strong, but still reject obviously wrong products.
        if not _is_confident_title_match(target, title, [slug], game_data.get("namespace", "")):
            if _slugify(target) != _slugify(slug) and target != game_data.get("namespace"):
                log.debug("Direct product '%s' rejected for target '%s' (title=%s)", slug, target, title)
                continue

        result = build_result_from_store_details(api, scraper, product, game_data)
        if result:
            return result, game_data.get("namespace", "")

    return None, ""

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
                "trailers", "screenshots", "logo", "poster", "background"):
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

    target = str(game_title or "").strip()
    if not target:
        return None

    # 1) Direct path: slug or namespace -> product page.
    # This avoids Epic search returning unrelated games for short names/codenames
    # e.g. 'rose'/'kine' resolving to Expeditions: Rome/Airborne Kingdom.
    direct_result, direct_namespace = fetch_direct_product_result(api, scraper, target)
    if direct_result:
        return enrich_from_base_game(direct_result, is_enriching, direct_namespace)

    # 2) Search path with strict confidence gate.
    search   = api.fetch_store_games(keywords=target, count=20)
    elements = search.get("data", {}).get("Catalog", {}).get("searchStore", {}).get("elements", [])
    if not elements:
        return None

    scored_elements = [
        (item, _match_score_for_item(item, target, target_namespace))
        for item in elements
    ]
    safe_elements = [item for item, score in scored_elements if score < 999]

    if not safe_elements:
        log.warning("No confident Epic match for '%s'. Refusing unsafe search result.", target)
        return None

    safe_elements.sort(key=lambda x: (
        _match_score_for_item(x, target, target_namespace),
        x.get("offerType") != "BASE_GAME",
        x.get("offerType") == "ADD_ON",
    ))

    # ── PRIMARY PATH: store pages available ──────────────────────────────────
    game_data, store_details = None, None
    for item in safe_elements:
        if details := fetch_store_details(api, item):
            game_data, store_details = item, details
            break

    if store_details and game_data:
        result = build_result_from_store_details(api, scraper, store_details, game_data)
        if result:
            return enrich_from_base_game(result, is_enriching, game_data.get("namespace", ""))

    # ── FALLBACK PATH: build from raw catalog data ───────────────────────────
    best_result, best_score, best_namespace = None, -1, ""

    for item in safe_elements:
        namespace  = item.get("namespace", "")
        catalog_id = item.get("id", "")

        if target_namespace and namespace != target_namespace:
            continue

        with ThreadPoolExecutor(max_workers=4) as ex:
            f_offer   = ex.submit(fetch_catalog_offer, scraper, namespace, catalog_id) if (namespace and catalog_id) else None
            f_rating  = ex.submit(fetch_epic_rating,   scraper, namespace) if namespace else None
            f_config  = ex.submit(fetch_store_config,  scraper, namespace) if namespace else None
            f_home    = ex.submit(fetch_home_config,   scraper, namespace) if namespace else None

        offer        = (f_offer.result()  if f_offer  else None) or {}
        real_rating  = (f_rating.result() if f_rating else 0.0)
        store_config = (f_config.result() if f_config else None)
        home_data    = (f_home.result()   if f_home   else {"screenshots": [], "video_data": []})

        screenshots: list[str]  = home_data.get("screenshots", [])
        trailers:    list[dict] = []

        for v_item in home_data.get("video_data", []):
            if ref_id := resolve_video_ref_id(scraper, v_item["id"]):
                if vid := fetch_video_url(scraper, ref_id):
                    if not vid.get("thumbnail") or "-00001-" in vid["thumbnail"]:
                        vid["thumbnail"] = v_item.get("cover", "")
                    trailers.append(vid)

        result               = build_result_from_catalog(offer, item, store_config, trailers, screenshots)
        result["avg_rating"] = real_rating

        # Final safety: built result must still match requested target.
        if not _is_confident_title_match(target, result.get("title", ""), get_slugs(item), namespace, target_namespace):
            continue

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