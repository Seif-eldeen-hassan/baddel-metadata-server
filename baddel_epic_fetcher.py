import sys
import json
import time
import re
import cloudscraper
import concurrent.futures
import urllib.parse
from datetime import datetime
from epicstore_api import EpicGamesStoreAPI

# ===========================================================
# Constants & Configurations
# ===========================================================
VALID_IMAGE_TYPES  = {"Screenshot", "GalleryImage"}
BAD_FILENAME_HINTS = {"usk", "pegi", "esrb", "cero", "rating", "ic1-", "icon",
                      "portrait", "-tall-", "offerimage", "banner"}
VIDEO_GRAPHQL_URL  = "https://store.epicgames.com/graphql"
VIDEO_QUERY_HASH   = "e631c6a22d716a93d05bcb023b0ef4ade869f5c2c241d88faf9187b51b282236"
CATALOG_GRAPHQL_URL = "https://store.epicgames.com/graphql"
CATALOG_QUERY_HASH  = "ec112951b1824e1e215daecae17db4069c737295d4a697ddb9832923f93a326e"

# ===========================================================
# 1. Slug & Core Helpers
# ===========================================================
def _is_dead_slug(slug: str) -> bool:
    return len(slug) == 32 and slug.isalnum()

def _clean_slug(slug: str) -> str:
    slug = slug.replace("/home", "")
    return slug[2:] if slug.startswith("p/") else slug

def get_slugs(item: dict) -> list[str]:
    raw = []
    for mapping in item.get("catalogNs", {}).get("mappings", []):
        if s := mapping.get("pageSlug", ""): raw.append(s)
    for mapping in item.get("offerMappings", []):
        if s := mapping.get("pageSlug", ""): raw.append(s)
    for key in ("productSlug", "urlSlug"):
        if s := item.get(key, ""): raw.append(s)
        
    result = []
    for s in raw:
        s = _clean_slug(s)
        if s not in result: result.append(s)
    return result

def pick_best_page(pages: list) -> dict:
    valid_pages = [p for p in pages if p.get("data")]
    if not valid_pages: return {}

    # Priority 1: Specifically target the base game "home" page
    for p in valid_pages:
        if p.get("type") == "productHome" or p.get("_slug") == "home":
            return p

    # Priority 2: Fallback to the shortest slug to avoid ultimate/deluxe editions
    valid_pages.sort(key=lambda p: len(p.get("_slug", p.get("pageName", "x"*100))))
    return valid_pages[0]


def get_thumbnail_mapping(page_node: dict) -> dict:
    """بتربط كل فيديو بالصورة الحقيقية بتاعته بدل الصورة السودة"""
    mapping = {}
    
    def _walk(node, current_image=""):
        if isinstance(node, dict):
            # بنمسك الـ Cover Image لو موجودة جنب الفيديو
            img = node.get("image", {}).get("src")
            if img: 
                current_image = img
            
            if "recipes" in node:
                recipes_str = node["recipes"]
                if isinstance(recipes_str, str) and "{" in recipes_str:
                    try:
                        data = json.loads(recipes_str)
                        for formats in data.values():
                            if not isinstance(formats, list): continue
                            for fmt in formats:
                                if not isinstance(fmt, dict): continue
                                ref_id = fmt.get("mediaRefId")
                                if ref_id:
                                    # الأولية 1: الصورة النظيفة من جوا الـ outputs
                                    thumb = next((o["url"] for o in fmt.get("outputs", []) if o.get("key") == "thumbnail" and o.get("url")), "")
                                    # الأولية 2: نستخدم الـ Cover Image اللي مسكناه
                                    if not thumb: thumb = current_image
                                    
                                    if thumb: mapping[ref_id] = thumb
                    except json.JSONDecodeError:
                        pass
                        
            elif "mediaRefId" in node and isinstance(node["mediaRefId"], str):
                if current_image:
                    mapping[node["mediaRefId"]] = current_image
                    
            for v in node.values(): 
                _walk(v, current_image)
                
        elif isinstance(node, list):
            for item in node: 
                _walk(item, current_image)
                
    _walk(page_node)
    return mapping

# ===========================================================
# 2. Epic Games API Fetchers
# ===========================================================
def fetch_epic_rating(scraper: cloudscraper.CloudScraper, sandbox_id: str) -> float:
    query = """
    query getProductResult($sandboxId: String!, $locale: String!) {
      RatingsPolls {
        getProductResult(sandboxId: $sandboxId, locale: $locale) {
          averageRating
        }
      }
    }
    """
    payload = {
        "operationName": "getProductResult",
        "query": query.strip(),
        "variables": {"sandboxId": sandbox_id, "locale": "en-US"} 
    }
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    
    try:
        res = scraper.post(CATALOG_GRAPHQL_URL, json=payload, headers=headers, timeout=8)
        if res.status_code == 200:
            rating = res.json().get("data", {}).get("RatingsPolls", {}).get("getProductResult", {}).get("averageRating")
            if rating:
                return round(float(rating), 1)
    except Exception:
        pass
    return 0.0

def fetch_home_config(scraper: cloudscraper.CloudScraper, sandbox_id: str) -> dict:
    HOME_CONFIG_HASH = "5a922bd3e5c84b60a4f443a019ef640b05cb0ae379beb4aca4515bf9812dfcb4"
    params = {
        "operationName": "getProductHomeConfig",
        "variables": json.dumps({"locale": "en-US", "sandboxId": sandbox_id}),
        "extensions": json.dumps({"persistedQuery": {"version": 1, "sha256Hash": HOME_CONFIG_HASH}})
    }
    result = {"screenshots": [], "video_data": []} 
    try:
        res = scraper.get(CATALOG_GRAPHQL_URL, params=params, timeout=8)
        if res.status_code == 200:
            data = res.json().get("data", {}).get("Product", {}).get("sandbox", {}).get("configuration", [])
            for config in data:
                for img in config.get("configs", {}).get("keyImages", []):
                    url = img.get("url", "")
                    if "com.epicgames.video://" in url:
                        parts = url.split("com.epicgames.video://")[1].split("?cover=")
                        result["video_data"].append({
                            "id": parts[0],
                            "cover": urllib.parse.unquote(parts[1]) if len(parts) > 1 else ""
                        })
                    elif img.get("type") == "featuredMedia":
                        result["screenshots"].append(url)
    except Exception:
        pass
    return result

def resolve_video_ref_id(scraper: cloudscraper.CloudScraper, video_item_id: str) -> str | None:
    query = """
    query getVideoById($videoId: String!, $locale: String!) {
      Video { fetchVideoByLocale(videoId: $videoId, locale: $locale) { recipe mediaRefId } }
    }
    """
    payload = {
        "query": query.strip(),
        "variables": {"videoId": video_item_id, "locale": "en-US"}
    }
    try:
        res = scraper.post(CATALOG_GRAPHQL_URL, json=payload, timeout=8)
        if res.status_code == 200:
            for item in res.json().get("data", {}).get("Video", {}).get("fetchVideoByLocale", []):
                if item.get("mediaRefId"):
                    return item["mediaRefId"]
    except Exception:
        pass
    return None

def fetch_store_config(scraper: cloudscraper.CloudScraper, sandbox_id: str) -> dict | None:
    STORE_CONFIG_HASH = "6a3c3cf307f98388bbb9c6958e2d4299e1e454b58951b1903439f1b7c1f74716"
    params = {
        "operationName": "getStoreConfig",
        "variables": json.dumps({"locale": "en-US", "sandboxId": sandbox_id}),
        "extensions": json.dumps({"persistedQuery": {"version": 1, "sha256Hash": STORE_CONFIG_HASH}})
    }
    try:
        res = scraper.get(CATALOG_GRAPHQL_URL, params=params, timeout=8)
        if res.status_code == 200:
            for config in res.json().get("data", {}).get("Product", {}).get("sandbox", {}).get("configuration", []):
                if "configs" in config: return config["configs"]
    except Exception:
        pass
    return None

def fetch_store_details(api: EpicGamesStoreAPI, item: dict) -> dict | None:
    for slug in get_slugs(item):
        try:
            if details := api.get_product(slug):
                if details.get("pages"): return details
        except Exception:
            continue
    return None

def fetch_catalog_offer(scraper: cloudscraper.CloudScraper, namespace: str, catalog_id: str) -> dict | None:
    params = {
        "operationName": "getCatalogOffer",
        "variables": json.dumps({"locale": "en-US", "country": "US", "offerId": catalog_id, "sandboxId": namespace}),
        "extensions": json.dumps({"persistedQuery": {"version": 1, "sha256Hash": CATALOG_QUERY_HASH}}),
    }
    try:
        res = scraper.get(CATALOG_GRAPHQL_URL, params=params, timeout=8)
        if res.status_code == 200:
            return res.json().get("data", {}).get("Catalog", {}).get("catalogOffer")
    except Exception:
        pass
    return None

# ===========================================================
# 3. Data Extraction (Primary Path)
# ===========================================================
def extract_metadata(page_node: dict, game_data: dict, offer: dict = None) -> dict:
    page_data = page_node.get("data", {})
    meta = page_data.get("meta", {})
    about = page_data.get("about", {})
    offer = offer or {}

    short_desc = about.get("shortDescription") or game_data.get("description", "")
    if short_desc.strip().lower() == game_data.get("title", "").strip().lower():
        short_desc = ""

    release_year = ""
    raw_date = (meta.get("releaseDate") or game_data.get("releaseDate") or game_data.get("effectiveDate") or "")
    if raw_date and not raw_date.startswith("2099"):
        try: release_year = str(datetime.fromisoformat(raw_date.replace("Z", "+00:00")).year)
        except Exception: release_year = raw_date[:4]

    developer, publisher = meta.get("developer") or [], meta.get("publisher") or []
    if not developer and about.get("developerAttribution"): developer = [about["developerAttribution"]]
    if not publisher and about.get("publisherAttribution"): publisher = [about["publisherAttribution"]]

    if not developer or not publisher:
        for attr in game_data.get("customAttributes", []):
            key, val = attr.get("key", "").lower(), attr.get("value", "")
            if not developer and "developer" in key: developer = [v.strip() for v in val.split(",")]
            if not publisher and "publisher" in key: publisher = [v.strip() for v in val.split(",")]

    # 🛠️ استخراج الـ Tags بشكل صارم
    genres, features = [], []
    FEATURE_TAGS = {"CLOUD_SAVES", "CONTROLLER", "SINGLE_PLAYER", "MULTI_PLAYER", "CO_OP", "CROSS_PLATFORM", "ACHIEVEMENTS", "LEADERBOARDS"}
    
    tags_source = offer.get("tags") or game_data.get("tags") or []
    
    for tag in tags_source:
        name = tag.get("name", "")
        group = (tag.get("groupName") or "").lower()
        
        # تجاهل الـ IDs أو الداتا الفاضية
        if not name or name.isdigit():
            continue
            
        # إضافة الـ Genres الرسمية فقط
        if "genre" in group: 
            genres.append(name.title())
            
        # إضافة الـ Features الرسمية أو اللي موجودة في القائمة بتاعتنا حتى لو من غير جروب
        elif "feature" in group or name.upper().replace(" ", "_") in FEATURE_TAGS: 
            features.append(name.title())
            

    return {
        "release_year": release_year,
        "developer": list(dict.fromkeys(developer)),
        "publisher": list(dict.fromkeys(publisher)),
        "platform": meta.get("platform") or [],
        "genres": list(dict.fromkeys(genres)),
        "features": list(dict.fromkeys(features)),
        "description": about.get("description", "") or game_data.get("description", ""),
        "short_description": short_desc,
    }


def extract_requirements(store_details: dict) -> dict:
    pages = store_details.get("pages", [])
    reqs = pick_best_page(pages).get("data", {}).get("requirements", {})
    if not reqs:
        for page in pages:
            if r := page.get("data", {}).get("requirements", {}):
                reqs = r
                break

    systems = {}
    for system in reqs.get("systems", []):
        sys_type = system.get("systemType", "Unknown")
        minimum, recommended = {}, {}
        for detail in system.get("details", []):
            if title := detail.get("title", ""):
                minimum[title] = detail.get("minimum", "")
                recommended[title] = detail.get("recommended", "")
        systems[sys_type] = {"minimum": minimum, "recommended": recommended}

    return {"languages": reqs.get("languages", []), "systems": systems}

def extract_ratings(store_details: dict) -> list[dict]:
    pages = store_details.get("pages", [])
    raw_ratings = pick_best_page(pages).get("productRatings", {}).get("ratings", [])
    if not raw_ratings:
        for page in pages:
            if r := page.get("productRatings", {}).get("ratings", []):
                raw_ratings = r
                break

    seen_titles, result = set(), []
    for r in raw_ratings:
        title = r.get("title", "")
        if title and title not in seen_titles:
            seen_titles.add(title)
            result.append({"title": title, "country_codes": r.get("countryCodes", ""), "image": r.get("image", {}).get("src", "")})
    return result

def _is_bad_image(url: str) -> bool:
    return any(hint in url.lower() for hint in BAD_FILENAME_HINTS)

def collect_screenshots(page_node: dict, game_data: dict, exclude: set) -> list[str]:
    seen, results = set(exclude), []
    def _add(url: str):
        if url and url not in seen and not _is_bad_image(url):
            seen.add(url)
            results.append(url)

    for item in page_node.get("data", {}).get("carousel", {}).get("items", []): _add(item.get("image", {}).get("src", ""))
    for url in page_node.get("_images_", []): _add(url)
    for img in game_data.get("keyImages", []):
        if img.get("type") in VALID_IMAGE_TYPES: _add(img.get("url", ""))

    return results[:10]

# ===========================================================
# 4. Trailers Extraction
# ===========================================================
def _parse_recipes(recipes) -> str | None:
    if isinstance(recipes, str) and "{" in recipes:
        try: recipes = json.loads(recipes)
        except json.JSONDecodeError: return None
    if not isinstance(recipes, dict): return None

    en_keys = [k for k in recipes if k.lower().startswith("en")]
    other_keys = [k for k in recipes if not k.lower().startswith("en")]

    for k in (en_keys + other_keys):
        formats = recipes.get(k, [])
        if not isinstance(formats, list): continue
        ref_by_recipe = {fmt["recipe"]: fmt["mediaRefId"] for fmt in formats if isinstance(fmt, dict) and fmt.get("recipe") and fmt.get("mediaRefId")}
        for recipe_type in ("video-fmp4", "video-hls", "video-webm"):
            if recipe_type in ref_by_recipe: return ref_by_recipe[recipe_type]
    return None

def _walk_recipes(node, ref_ids: list):
    if isinstance(node, dict):
        if "recipes" in node:
            if (ref_id := _parse_recipes(node["recipes"])) and ref_id not in ref_ids: ref_ids.append(ref_id)
        if "mediaRefId" in node:
            ref_id = node["mediaRefId"]
            if isinstance(ref_id, str) and len(ref_id) > 10 and ref_id not in ref_ids: ref_ids.append(ref_id)
        for v in node.values(): _walk_recipes(v, ref_ids)
    elif isinstance(node, list):
        for item in node: _walk_recipes(item, ref_ids)

def extract_page_ref_ids(page_node: dict) -> list[str]:
    # We rename this and ONLY pass the selected page, not the whole store_details
    ref_ids = []
    _walk_recipes(page_node, ref_ids)
    return ref_ids

def fetch_video_url(scraper: cloudscraper.CloudScraper, ref_id: str) -> dict | None:
    params = {
        "operationName": "getVideo",
        "variables": json.dumps({"mediaRefId": ref_id.replace("-", "")}),
        "extensions": json.dumps({"persistedQuery": {"version": 1, "sha256Hash": VIDEO_QUERY_HASH}})
    }
    try:
        res = scraper.get(VIDEO_GRAPHQL_URL, params=params, timeout=5)
        if res.status_code == 200:
            outputs = res.json().get("data", {}).get("Media", {}).get("getMediaRef", {}).get("outputs", [])
            by_key = {o["key"].lower(): o["url"] for o in outputs if o.get("key") and o.get("url")}
            if video_url := (by_key.get("manifest") or by_key.get("high")):
                return {"video": video_url, "thumbnail": by_key.get("image") or by_key.get("thumbnail") or ""}
    except Exception:
        pass
    return None

def fetch_all_trailers(scraper, ref_ids: list[str], thumb_map: dict = None) -> list[dict]:
    thumb_map = thumb_map or {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_ref = {executor.submit(fetch_video_url, scraper, rid): rid for rid in ref_ids}
        results = {}
        for future in concurrent.futures.as_completed(future_to_ref):
            rid = future_to_ref[future]
            if vid_dict := future.result():
                current_thumb = vid_dict.get("thumbnail", "")
                good_thumb = thumb_map.get(rid)
                
                # 🛠️ لو الصورة سودة (فيها -00001-) أو مش موجودة، حط الصورة النظيفة
                if good_thumb and (not current_thumb or "-00001-" in current_thumb):
                    vid_dict["thumbnail"] = good_thumb
                    
                results[rid] = vid_dict
                
    return [results[rid] for rid in ref_ids if rid in results]

# ===========================================================
# 5. Data Construction (Fallback Path)
# ===========================================================
def build_result_from_catalog(offer: dict, game_data: dict, store_config: dict = None, trailers: list = None, extracted_screenshots: list = None) -> dict:
    offer, store_config = offer or {}, store_config or {}
    
    release_year, raw_date = "", (offer.get("releaseDate") or offer.get("effectiveDate") or game_data.get("releaseDate") or game_data.get("effectiveDate") or store_config.get("pcReleaseDate", ""))
    if raw_date and raw_date.startswith("2099"): raw_date = ""
    if raw_date:
        try: release_year = str(datetime.fromisoformat(raw_date.replace("Z", "+00:00")).year)
        except Exception: release_year = raw_date[:4]

    developer, publisher = [], []
    if store_config.get("developerDisplayName"): developer = [store_config["developerDisplayName"]]
    if store_config.get("publisherDisplayName"): publisher = [store_config["publisherDisplayName"]]
    if not developer or not publisher:
        for attrs in [offer.get("customAttributes") or [], game_data.get("customAttributes") or []]:
            for attr in attrs:
                key, val = attr.get("key", "").lower(), attr.get("value", "")
                if not developer and "developer" in key: developer = [v.strip() for v in val.split(",") if v.strip()]
                if not publisher and "publisher" in key: publisher = [v.strip() for v in val.split(",") if v.strip()]
            if developer and publisher: break

    genres, features, platform = [], [], []
    for tag in (store_config.get("tags") or offer.get("tags") or game_data.get("tags") or []):
        if not tag: continue
        name, group = tag.get("name", ""), (tag.get("groupName") or "").lower()
        if not name: continue
        if "genre" in group: genres.append(name)
        elif "feature" in group: features.append(name)
        elif "platform" in group: platform.append(name)

    requirements = {"languages": [], "systems": {}}
    if tech := store_config.get("technicalRequirements", {}):
        for os_name in ["windows", "macos", "linux"]:
            if tech.get(os_name):
                sys_dict = {"minimum": {}, "recommended": {}}
                for req in tech[os_name]:
                    if title := req.get("title", ""):
                        sys_dict["minimum"][title] = req.get("minimum", "")
                        sys_dict["recommended"][title] = req.get("recommended", "")
                requirements["systems"]["Mac" if os_name == "macos" else os_name.capitalize()] = sys_dict

    langs = []
    if store_config.get("supportedAudio"): langs.append("AUDIO: " + ", ".join(store_config["supportedAudio"]))
    if store_config.get("supportedText"): langs.append("TEXT: " + ", ".join(store_config["supportedText"]))
    requirements["languages"] = langs

    if not platform:
        OS_DISPLAY = {"windows": "Windows", "mac": "Mac", "linux": "Linux"}
        for sys_key in requirements.get("systems", {}):
            if (display := OS_DISPLAY.get(sys_key.lower(), sys_key)) not in platform: platform.append(display)
    if not platform:
        for img in (offer.get("keyImages") or game_data.get("keyImages") or []):
            url = (img.get("url") or "").lower()
            if "win" in url and "Windows" not in platform: platform.append("Windows")
            elif "mac" in url and "Mac" not in platform: platform.append("Mac")

    key_images = offer.get("keyImages") or game_data.get("keyImages") or []
    poster_url = next((img["url"] for img in key_images if img and img.get("type") in {"OfferImageTall", "DieselStoreFrontTall"}), "")
    background_url = next((img["url"] for img in key_images if img and img.get("type") in {"DieselStoreFrontWide", "OfferImageWide"}), "")
    
    logo_url = next((img.get("url", "") for img in (store_config.get("keyImages") or []) if img and img.get("type") == "ProductLogo"), "")
    
    screenshots = extracted_screenshots or []
    if not screenshots:
        screenshots = [img["url"] for img in key_images if img and img.get("type") in VALID_IMAGE_TYPES and not _is_bad_image(img.get("url", ""))][:10]

    banner = store_config.get("banner") or {}
    description = (offer.get("longDescription") or game_data.get("longDescription") or banner.get("description") or 
                   offer.get("description") or store_config.get("description") or game_data.get("description", ""))

    return {
        "title": store_config.get("productDisplayName") or offer.get("title") or game_data.get("title", ""),
        "short_description": offer.get("description") or game_data.get("description", ""),
        "description": description,
        "avg_rating": offer.get("averageRating") or game_data.get("averageRating", 0),
        "release_year": release_year,
        "developer": list(dict.fromkeys(developer)),
        "publisher": list(dict.fromkeys(publisher)),
        "platform": list(dict.fromkeys(platform)),
        "genres": list(dict.fromkeys(genres)),
        "features": list(dict.fromkeys(features)),
        "ratings": [],          
        "requirements": requirements,
        "poster": poster_url,
        "background": background_url,
        "logo": logo_url,
        "trailers": trailers or [],          
        "screenshots": screenshots[:10],
    }

# ===========================================================
# 6. Main Flow Manager
# ===========================================================
def get_baddel_data(game_title: str) -> dict | None:
    api = EpicGamesStoreAPI()
    scraper = cloudscraper.create_scraper()

    search = api.fetch_store_games(keywords=game_title, count=10)
    elements = search.get("data", {}).get("Catalog", {}).get("searchStore", {}).get("elements", [])

    if not elements: return None

    target_lower = game_title.lower()
    elements.sort(key=lambda x: (
        x.get("title", "").lower() != target_lower,
        x.get("offerType") == "ADD_ON"
    ))

    game_data, store_details = None, None
    for item in elements:
        if details := fetch_store_details(api, item):
            game_data, store_details = item, details
            break
    
    # ===========================================================
    # 🛠️ DEBUGGING: Save RAW JSON to a file for inspection
    # ===========================================================
    if game_data and store_details:
        debug_dump = {
            "search_result_game_data": game_data,
            "store_details_pages": store_details.get("pages", [])
        }
        with open("debug_epic_raw.json", "w", encoding="utf-8") as f:
            json.dump(debug_dump, f, indent=2, ensure_ascii=False)
        print("🛠️ [DEBUG] تم حفظ الداتا الخام في ملف 'debug_epic_raw.json'. افتحه عشان تشوف المشكلة فين.", file=sys.stderr)
    # ===========================================================

    # ── PRIMARY PATH ──
    if store_details:
        page_node = pick_best_page(store_details["pages"])
        namespace = game_data.get("namespace", "")
        catalog_id = game_data.get("id", "") # 👈 هنجيب الـ ID بتاع اللعبة
        
        # 👈 هنكلم الـ GraphQL هنا عشان نجيب الداتا الكاملة للـ Tags
        offer_data = fetch_catalog_offer(scraper, namespace, catalog_id) if namespace and catalog_id else {}
        
        real_rating = fetch_epic_rating(scraper, namespace) if namespace else 0
        avg_rating = real_rating if real_rating > 0 else game_data.get("averageRating", 0)
        
        hero = page_node.get("data", {}).get("hero", {})
        logo_url = hero.get("logoImage", {}).get("src", "")
        background_url = hero.get("backgroundImageUrl", "")
        poster_url = next((img["url"] for img in game_data.get("keyImages", []) if img.get("type") in {"OfferImageTall", "DieselStoreFrontTall"}), "")
        
        exclude_set = {u for u in [logo_url, poster_url, background_url] if u}
        screenshots = collect_screenshots(page_node, game_data, exclude_set)

        # 👈 هنباصي الـ offer_data هنا
        meta = extract_metadata(page_node, game_data, offer_data) 
        
        requirements = extract_requirements(store_details)
        ratings = extract_ratings(store_details)

        direct_trailers = re.findall(r'https?://[^\s"\'\{\}\[\]]+\.mp4', json.dumps(page_node))
        trailers = list(dict.fromkeys(direct_trailers))
        
        if not trailers:
            ref_ids = extract_page_ref_ids(page_node)
            thumb_map = get_thumbnail_mapping(page_node) 
            trailers = fetch_all_trailers(scraper, ref_ids, thumb_map)

        return {
            "title": game_data.get("title"),
            "short_description": meta.get("short_description", ""),
            "description": meta["description"],
            "avg_rating": avg_rating,
            "release_year": meta["release_year"],
            "developer": meta["developer"],
            "publisher": meta["publisher"],
            "platform": meta["platform"],
            "genres": meta["genres"],
            "features": meta["features"],
            "ratings": ratings,
            "requirements": requirements,
            "poster": poster_url,
            "background": background_url,
            "logo": logo_url,
            "trailers": trailers,
            "screenshots": screenshots[:10]
        }

    # ── FALLBACK PATH ──
    best_fallback_result, best_score = None, -1

    for item in elements:
        namespace, catalog_id = item.get("namespace", ""), item.get("id", "")
        offer, trailers, screenshots = {}, [], []
        
        if namespace and catalog_id:
            if fetched_offer := fetch_catalog_offer(scraper, namespace, catalog_id): offer = fetched_offer
                
        real_rating = fetch_epic_rating(scraper, namespace)
        store_config = fetch_store_config(scraper, namespace)
        home_config_data = fetch_home_config(scraper, namespace)
        screenshots = home_config_data.get("screenshots", [])
        
        if v_data := home_config_data.get("video_data", []):
            for v_item in v_data:
                if ref_id := resolve_video_ref_id(scraper, v_item["id"]):
                    if vid_url_info := fetch_video_url(scraper, ref_id):
                        if not vid_url_info.get("thumbnail") or "-00001-" in vid_url_info["thumbnail"]:
                            vid_url_info["thumbnail"] = v_item["cover"]
                        trailers.append(vid_url_info)

        result = build_result_from_catalog(offer, item, store_config, trailers, screenshots)
        result["avg_rating"] = real_rating 
        
        desc, title = result.get("description", ""), result.get("title", "")
        has_screenshots = len(result.get("screenshots", [])) > 0
        has_real_desc = desc and len(desc) > 40 and desc.strip().lower() != title.strip().lower()
        current_score = (len(result.get("screenshots", [])) * 100) + len(desc)

        if current_score > best_score:
            best_fallback_result, best_score = result, current_score
            
        if has_screenshots or has_real_desc:
            return result

    return best_fallback_result


# ===========================================================
# Entry Point
# ===========================================================
if __name__ == "__main__":
    target = " ".join(sys.argv[1:]) or "Control"
    
    start_time = time.time()  # ⏱️ بداية حساب الوقت
    result = get_baddel_data(target)
    end_time = time.time()    # ⏱️ نهاية حساب الوقت
    
    if result:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print(f"\n⏱️ Time Taken: {end_time - start_time:.2f} seconds", file=sys.stderr)