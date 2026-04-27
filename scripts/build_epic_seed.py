import argparse
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from epicstore_api import EpicGamesStoreAPI


DEFAULT_OUT = "data/epic_seed.json"
DEFAULT_REJECTED_OUT = "data/epic_seed_rejected.json"
DEFAULT_STATS_OUT = "data/epic_seed_stats.json"

JUNK_RE = re.compile(
    r"\b("
    r"demo|trial|playtest|test|beta|alpha|server|dedicated server|"
    r"soundtrack|ost|sdk|editor|trailer|teaser|"
    r"dlc|add-on|addon|add on|season pass|battle pass|"
    r"currency|coins|credits|v-bucks|pack|bundle|upgrade|"
    r"skin|cosmetic|outfit|weapon pack|starter pack"
    r")\b",
    re.IGNORECASE,
)


def title_from_slug(slug: str) -> str:
    """
    Convert Epic slug/path into a readable title fallback.
    Example:
      "grand-theft-auto-v" -> "Grand Theft Auto V"
      "/p/alan-wake-2"     -> "Alan Wake 2"
    """
    if not slug:
        return ""

    value = str(slug).strip().strip("/")
    value = value.split("/")[-1]
    value = value.split("?")[0]
    value = value.replace("-", " ").replace("_", " ")
    value = re.sub(r"\s+", " ", value).strip()

    if not value:
        return ""

    # Keep common roman numerals looking better
    words = []
    for word in value.split(" "):
        upper = word.upper()
        if upper in {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"}:
            words.append(upper)
        else:
            words.append(word.capitalize())

    return " ".join(words)


def clean_title(title: str) -> str:
    if not title:
        return ""

    title = str(title)
    title = re.sub(r"\s+", " ", title)
    title = title.strip(" \t\r\n-–—|")
    return title


def normalize_mapping_item(namespace: Any, slug: Any) -> Optional[Dict[str, str]]:
    if not namespace or not slug:
        return None

    namespace = str(namespace).strip()
    slug = str(slug).strip()

    if not namespace or not slug:
        return None

    title = title_from_slug(slug)

    if not title:
        title = namespace

    return {
        "id": namespace,
        "title": clean_title(title),
        "slug": slug,
    }


def is_junk(title: str, slug: str) -> bool:
    haystack = f"{title} {slug}"
    return bool(JUNK_RE.search(haystack))


def pick_first_string(*values: Any) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def extract_title_from_product(product: Dict[str, Any]) -> str:
    """
    epicstore_api product response shape can vary, so this tries common fields.
    If it fails, caller falls back to slug title.
    """
    if not isinstance(product, dict):
        return ""

    direct = pick_first_string(
        product.get("title"),
        product.get("name"),
        product.get("productName"),
    )
    if direct:
        return clean_title(direct)

    # Sometimes data is nested.
    for key in ["data", "product", "page", "catalogItem"]:
        nested = product.get(key)
        if isinstance(nested, dict):
            nested_title = pick_first_string(
                nested.get("title"),
                nested.get("name"),
                nested.get("productName"),
            )
            if nested_title:
                return clean_title(nested_title)

    return ""


def product_looks_like_game(product: Dict[str, Any]) -> Optional[bool]:
    """
    Returns:
      True  -> likely game
      False -> likely add-on/non-game
      None  -> unknown, don't reject based on this
    """
    if not isinstance(product, dict):
        return None

    text_bits = []

    def collect(obj: Any, depth: int = 0) -> None:
        if depth > 3:
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                key_l = str(key).lower()
                if key_l in {
                    "producttype",
                    "producttypecode",
                    "offeretype",
                    "offerType".lower(),
                    "categories",
                    "category",
                    "tags",
                    "title",
                    "name",
                }:
                    text_bits.append(str(value))
                if isinstance(value, (dict, list)):
                    collect(value, depth + 1)
        elif isinstance(obj, list):
            for item in obj[:20]:
                collect(item, depth + 1)

    collect(product)

    text = " ".join(text_bits).lower()

    if any(x in text for x in ["addon", "add-on", "dlc", "consumable", "currency"]):
        return False

    if any(x in text for x in ["base_game", "base game", "game"]):
        return True

    return None


def fetch_product_details(
    api: EpicGamesStoreAPI,
    slug: str,
    retries: int = 2,
    delay: float = 1.0,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            product = api.get_product(slug)
            if isinstance(product, dict):
                return product, None
            return None, "product_response_not_dict"
        except Exception as exc:
            last_error = str(exc)
            if attempt < retries:
                time.sleep(delay * attempt)

    return None, last_error or "unknown_error"


def write_json(path: str, payload: Any) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def build_seed(args: argparse.Namespace) -> None:
    api = EpicGamesStoreAPI()

    print("[epic-seed] fetching product mapping...")
    mapping = api.get_product_mapping()

    if not isinstance(mapping, dict):
        raise RuntimeError("Epic product mapping response was not a dictionary.")

    raw_count = len(mapping)
    duplicate_count = 0
    empty_count = 0
    junk_count = 0
    detail_rejected_count = 0
    detail_error_count = 0
    kept_count = 0

    games = []
    rejected = []
    seen_namespaces = set()

    items = list(mapping.items())

    if args.limit and args.limit > 0:
        items = items[: args.limit]

    print(f"[epic-seed] raw mapping count: {raw_count}")
    print(f"[epic-seed] processing count: {len(items)}")

    for index, (namespace, slug) in enumerate(items, start=1):
        item = normalize_mapping_item(namespace, slug)

        if item is None:
            empty_count += 1
            rejected.append({
                "namespace": namespace,
                "slug": slug,
                "reason": "empty_namespace_or_slug",
            })
            continue

        if item["id"] in seen_namespaces:
            duplicate_count += 1
            rejected.append({
                **item,
                "reason": "duplicate_namespace",
            })
            continue

        if not args.include_junk and is_junk(item["title"], item["slug"]):
            junk_count += 1
            rejected.append({
                **item,
                "reason": "junk_name_filter",
            })
            continue

        if args.validate_details:
            if index % 25 == 0 or index == 1:
                print(f"[epic-seed] validating details {index}/{len(items)}...")

            product, error = fetch_product_details(
                api=api,
                slug=item["slug"],
                retries=args.retries,
                delay=args.retry_delay,
            )

            if error:
                detail_error_count += 1
                rejected.append({
                    **item,
                    "reason": "product_details_error",
                    "error": error,
                })
                if args.keep_on_detail_error:
                    seen_namespaces.add(item["id"])
                    games.append(item)
                    kept_count += 1
                time.sleep(args.delay)
                continue

            detail_game_check = product_looks_like_game(product or {})

            if detail_game_check is False:
                detail_rejected_count += 1
                rejected.append({
                    **item,
                    "reason": "product_details_non_game_or_addon",
                })
                time.sleep(args.delay)
                continue

            better_title = extract_title_from_product(product or {})
            if better_title and not is_junk(better_title, item["slug"]):
                item["title"] = better_title

            if args.delay > 0:
                time.sleep(args.delay)

        seen_namespaces.add(item["id"])

        # Output shape expected by seed_railway.js and your server:
        # id = Epic namespace
        # title = useful search/title hint
        games.append({
            "id": item["id"],
            "title": item["title"],
            "slug": item["slug"],
        })
        kept_count += 1

    stats = {
        "raw_mapping_count": raw_count,
        "processed_count": len(items),
        "kept_usable_entries": kept_count,
        "removed_empty": empty_count,
        "removed_duplicates": duplicate_count,
        "removed_junk_or_addons_by_name": junk_count,
        "removed_by_product_details": detail_rejected_count,
        "product_detail_errors": detail_error_count,
        "validate_details": args.validate_details,
        "include_junk": args.include_junk,
    }

    write_json(args.out, games)
    write_json(args.rejected_out, rejected)
    write_json(args.stats_out, stats)

    print("")
    print(f"[epic-seed] raw mapping count: {stats['raw_mapping_count']}")
    print(f"[epic-seed] processed count: {stats['processed_count']}")
    print(f"[epic-seed] removed empty: {stats['removed_empty']}")
    print(f"[epic-seed] removed duplicates: {stats['removed_duplicates']}")
    print(f"[epic-seed] removed junk/addons by name: {stats['removed_junk_or_addons_by_name']}")
    print(f"[epic-seed] removed by product details: {stats['removed_by_product_details']}")
    print(f"[epic-seed] product detail errors: {stats['product_detail_errors']}")
    print(f"[epic-seed] kept usable entries: {stats['kept_usable_entries']}")
    print("")
    print(f"[epic-seed] wrote seed to: {args.out}")
    print(f"[epic-seed] wrote rejected to: {args.rejected_out}")
    print(f"[epic-seed] wrote stats to: {args.stats_out}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Epic Games seed file for Baddel metadata server."
    )

    parser.add_argument(
        "--out",
        default=DEFAULT_OUT,
        help=f"Output seed JSON path. Default: {DEFAULT_OUT}",
    )
    parser.add_argument(
        "--rejected-out",
        default=DEFAULT_REJECTED_OUT,
        help=f"Rejected entries JSON path. Default: {DEFAULT_REJECTED_OUT}",
    )
    parser.add_argument(
        "--stats-out",
        default=DEFAULT_STATS_OUT,
        help=f"Stats JSON path. Default: {DEFAULT_STATS_OUT}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of mapping entries processed. Useful for testing.",
    )
    parser.add_argument(
        "--include-junk",
        action="store_true",
        help="Do not filter demo/DLC/addon/pack-like names.",
    )
    parser.add_argument(
        "--validate-details",
        action="store_true",
        help="Slow mode: call Epic product details for each slug to improve titles/filtering.",
    )
    parser.add_argument(
        "--keep-on-detail-error",
        action="store_true",
        help="In --validate-details mode, keep entries even if product detail fetch fails.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.35,
        help="Delay between product detail calls in --validate-details mode.",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=1.0,
        help="Delay multiplier between detail fetch retries.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Retries per product detail request.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_seed(args)


if __name__ == "__main__":
    main()