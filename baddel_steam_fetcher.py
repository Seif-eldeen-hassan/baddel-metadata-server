import requests
import json
import re
import sys

def parse_steam_requirements(html_str: str) -> dict:
    if not html_str or not isinstance(html_str, str):
        return {}
    KEY_MAP = {
        "os": "os_versions",
        "processor": "cpu",
        "memory": "ram",
        "graphics": "gpu",
        "storage": "storage",
        "hard drive": "storage",
        "directx": "directx",
        "additional notes": "notes",
        "sound card": "notes"
    }
    parsed_reqs = {}
    pattern = r'<strong>(.*?)[:]?<\/strong>\s*(.*?)(?:<br\s*\/?>|<\/li>|$)'
    matches = re.findall(pattern, html_str, re.IGNORECASE)
    for key, value in matches:
        clean_key = key.strip().lower()
        clean_value = re.sub(r'<[^>]+>', '', value).strip()
        if clean_key in KEY_MAP:
            db_key = KEY_MAP[clean_key]
            if db_key == 'notes' and 'notes' in parsed_reqs:
                parsed_reqs[db_key] += " | " + clean_value
            else:
                parsed_reqs[db_key] = clean_value

    return parsed_reqs

def clean_html(raw_html):
    if not raw_html: return ""
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext.replace('\t', '').replace('\n', ' ').strip()

def clean_description(raw_html):
    if not raw_html: return ""
    text = re.sub(r'<(br|/p|/h[1-6]|/div|/li)\s*/?>', '\n', raw_html, flags=re.IGNORECASE)
    cleanr = re.compile('<.*?>')
    text = re.sub(cleanr, '', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.replace('\t', '').strip()

def get_steam_game_standardized(app_id):
    details_url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&l=english"
    reviews_url = f"https://store.steampowered.com/appreviews/{app_id}?json=1&language=all"

    try:
        # ── Details request ──────────────────────────────────────────────────
        details_raw = requests.get(details_url, timeout=15)

        content_type = details_raw.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            print(json.dumps({
                "error": f"Steam returned non-JSON (HTTP {details_raw.status_code}), likely rate limited"
            }))
            return

        details_res = details_raw.json()

        if str(app_id) not in details_res or not details_res[str(app_id)].get('success'):
            print(json.dumps({"error": "Invalid AppID or game unavailable"}))
            return

        data = details_res[str(app_id)]['data']

        # ── Reviews request ──────────────────────────────────────────────────
        reviews_raw = requests.get(reviews_url, timeout=15)
        reviews_res = reviews_raw.json() if 'application/json' in reviews_raw.headers.get('Content-Type', '') else {}
        reviews_data = reviews_res.get('query_summary', {})

        # ── Parse fields ─────────────────────────────────────────────────────
        release_date_str = data.get('release_date', {}).get('date', '')
        release_year = release_date_str[-4:] if release_date_str else ""

        platforms_dict = data.get('platforms', {})
        platforms = [plat.capitalize() for plat, supported in platforms_dict.items() if supported]

        languages_raw = data.get('supported_languages', '')
        languages_clean = clean_html(languages_raw).replace('*', '')

        trailers = []
        for movie in data.get('movies', []):
            video_url = movie.get('dash_h264', '') or movie.get('mp4', {}).get('max', movie.get('webm', {}).get('max', ''))
            trailers.append({"video": video_url, "thumbnail": movie.get('thumbnail', '')})

        screenshots = [{"url": ss.get('path_full', ''), "width": 1920, "height": 1080} for ss in data.get('screenshots', [])]

        systems = {}
        for os_name, os_key in [("Windows", "pc_requirements"), ("macOS", "mac_requirements"), ("Linux", "linux_requirements")]:
            reqs = data.get(os_key, {})
            if reqs and isinstance(reqs, dict) and (reqs.get('minimum') or reqs.get('recommended')):
                min_html = reqs.get('minimum', '')
                rec_html = reqs.get('recommended', '')
                systems[os_name] = {}
                if min_html: systems[os_name]["minimum"] = parse_steam_requirements(min_html)
                if rec_html: systems[os_name]["recommended"] = parse_steam_requirements(rec_html)

        vertical_poster = f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{app_id}/library_600x900_2x.jpg"
        transparent_logo = f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{app_id}/logo.png"

        cleaned_long_desc = clean_description(data.get('detailed_description', ''))
        final_description = data.get('short_description', '') if len(cleaned_long_desc) < 50 else cleaned_long_desc

        baddel_format = {
            "title": data.get('name', ''),
            "short_description": data.get('short_description', ''),
            "description": final_description,
            "rating": {
                "score": data.get('metacritic', {}).get('score', 0) if data.get('metacritic') else 0,
                "max_score": 100,
                "source": "Metacritic" if data.get('metacritic') else "No Rating"
            },
            "release_year": release_year,
            "developer": data.get('developers', []),
            "publisher": data.get('publishers', []),
            "platform": platforms,
            "genres": [genre['description'] for genre in data.get('genres', [])],
            "features": [cat['description'] for cat in data.get('categories', [])],
            "requirements": {
                "languages": [languages_clean],
                "systems": systems
            },
            "poster": vertical_poster,
            "background": data.get('background_raw', data.get('background', '')),
            "logo": transparent_logo,
            "trailers": trailers,
            "screenshots": screenshots,
            "steam_reviews": {
                "review_score_desc": reviews_data.get('review_score_desc', 'No Reviews'),
                "total_reviews": reviews_data.get('total_reviews', 0),
                "positive_reviews": reviews_data.get('total_positive', 0),
                "negative_reviews": reviews_data.get('total_negative', 0)
            }
        }

        print(json.dumps(baddel_format, ensure_ascii=False))

    except Exception as e:
        print(json.dumps({"error": str(e)}))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        app_id = sys.argv[1]
        get_steam_game_standardized(app_id)
    else:
        print(json.dumps({"error": "No AppID provided"}))