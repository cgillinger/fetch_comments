#!/usr/bin/env python3
"""Download ALL comments from Facebook Pages using the Meta Graph API."""

import argparse
import csv
import io
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GRAPH_API_BASE = "https://graph.facebook.com/v22.0"
DEFAULT_LIMIT = 100
MAX_RETRIES = 8
INITIAL_BACKOFF = 2
MAX_BACKOFF = 120
USAGE_THRESHOLD = 80
THROTTLE_DELAY_MIN = 5
THROTTLE_DELAY_MAX = 15
CHECKPOINT_FILE = "checkpoint.json"

COMMENT_FIELDS = (
    "id,message,created_time,permalink_url,"
    "from{id,name,link},like_count,comment_count,"
    "is_hidden,parent,attachment,message_tags,"
    "reactions.summary(true)"
)

POST_FIELDS = "id,created_time,permalink_url,status_type,attachments"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler("fetch_comments.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
request_counter = 0
counter_lock = threading.Lock()
seen_comment_ids: set[str] = set()
seen_lock = threading.Lock()
shutdown_event = threading.Event()


def _inc_counter() -> int:
    global request_counter
    with counter_lock:
        request_counter += 1
        return request_counter


# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------
def _signal_handler(sig: int, frame: Any) -> None:
    logger.warning("Interrupt received – shutting down gracefully …")
    shutdown_event.set()


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------------------------------------------------------------------------
# Rate-limit / usage helpers
# ---------------------------------------------------------------------------

def _check_usage_headers(resp: requests.Response) -> None:
    """If X-App-Usage or X-Page-Usage exceeds threshold, insert delay."""
    for header_name in ("X-App-Usage", "X-Page-Usage"):
        raw = resp.headers.get(header_name)
        if not raw:
            continue
        try:
            usage = json.loads(raw)
            for key in ("call_count", "total_cputime", "total_time"):
                val = usage.get(key, 0)
                if val > USAGE_THRESHOLD:
                    delay = random.uniform(THROTTLE_DELAY_MIN, THROTTLE_DELAY_MAX)
                    logger.warning(
                        "Usage header %s.%s=%s > %s%% – sleeping %.1fs",
                        header_name, key, val, USAGE_THRESHOLD, delay,
                    )
                    time.sleep(delay)
                    return
        except (json.JSONDecodeError, TypeError):
            pass


def _is_rate_limit_error(resp: requests.Response) -> bool:
    if resp.status_code == 429:
        return True
    try:
        body = resp.json()
        code = body.get("error", {}).get("code")
        if code in (4, 17, 32):
            return True
    except (ValueError, AttributeError):
        pass
    return False


# ---------------------------------------------------------------------------
# Core HTTP helper
# ---------------------------------------------------------------------------

def api_get(
    session: requests.Session,
    url: str,
    params: Optional[dict[str, Any]] = None,
    delay: float = 0.0,
) -> dict[str, Any]:
    """GET with retry + exponential backoff on rate-limit errors."""
    if delay > 0:
        time.sleep(delay)

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        if shutdown_event.is_set():
            raise SystemExit("Shutdown requested")

        count = _inc_counter()
        logger.debug("Request #%d  GET %s", count, url[:120])

        try:
            resp = session.get(url, params=params, timeout=60)
        except requests.RequestException as exc:
            logger.error("Network error (attempt %d/%d): %s", attempt, MAX_RETRIES, exc)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
            continue

        if _is_rate_limit_error(resp):
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                wait = int(retry_after)
            else:
                wait = backoff
            logger.warning(
                "Rate-limited (attempt %d/%d) – sleeping %ds", attempt, MAX_RETRIES, wait,
            )
            if attempt == MAX_RETRIES:
                resp.raise_for_status()
            time.sleep(wait)
            backoff = min(backoff * 2, MAX_BACKOFF)
            continue

        _check_usage_headers(resp)
        resp.raise_for_status()
        return resp.json()

    raise RuntimeError("Exceeded max retries for request")


def paginate_all(
    session: requests.Session,
    url: str,
    params: Optional[dict[str, Any]] = None,
    delay: float = 0.0,
) -> list[dict[str, Any]]:
    """Follow paging.next until exhausted. Returns aggregated data list."""
    results: list[dict[str, Any]] = []
    current_url = url
    current_params = params

    while True:
        if shutdown_event.is_set():
            break
        body = api_get(session, current_url, params=current_params, delay=delay)
        data = body.get("data", [])
        results.extend(data)

        next_url = body.get("paging", {}).get("next")
        if not next_url:
            break
        current_url = next_url
        current_params = None  # params are already baked into next_url

    return results


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint(path: str) -> dict[str, Any]:
    p = Path(path)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(path: str, data: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

CSV_COLUMNS = [
    "page_id",
    "page_name",
    "post_id",
    "post_created_time",
    "post_permalink",
    "comment_id",
    "parent_comment_id",
    "depth_level",
    "created_time",
    "message",
    "commenter_id",
    "commenter_name",
    "commenter_profile_link",
    "like_count",
    "reaction_count",
    "reply_count",
    "is_hidden",
    "attachment_type",
    "message_tags",
]


class StreamWriters:
    """Writes records as NDJSON and CSV in streaming fashion."""

    def __init__(self, ndjson_path: str, csv_path: str) -> None:
        self._ndjson_fh = open(ndjson_path, "a", encoding="utf-8")
        self._csv_fh = open(csv_path, "a", encoding="utf-8", newline="")
        self._csv_writer = csv.DictWriter(
            self._csv_fh, fieldnames=CSV_COLUMNS, extrasaction="ignore",
        )
        if self._csv_fh.tell() == 0:
            self._csv_writer.writeheader()
        self._lock = threading.Lock()

    def write(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._ndjson_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            self._csv_writer.writerow(record)

    def flush(self) -> None:
        with self._lock:
            self._ndjson_fh.flush()
            self._csv_fh.flush()

    def close(self) -> None:
        self._ndjson_fh.close()
        self._csv_fh.close()


# ---------------------------------------------------------------------------
# Comment extraction
# ---------------------------------------------------------------------------

def _extract_comment_record(
    comment: dict[str, Any],
    page_id: str,
    page_name: str,
    post_id: str,
    post_created_time: str,
    post_permalink: str,
    parent_comment_id: Optional[str],
    depth_level: int,
) -> dict[str, Any]:
    from_data = comment.get("from") or {}
    commenter_id = from_data.get("id", "")
    commenter_name = from_data.get("name", "")
    commenter_link = from_data.get("link", "")
    if not commenter_link and commenter_id:
        commenter_link = f"https://facebook.com/{commenter_id}"

    attachment = comment.get("attachment") or {}
    attachment_type = attachment.get("type", "")

    reactions_summary = (comment.get("reactions") or {}).get("summary", {})
    reaction_count = reactions_summary.get("total_count", 0)

    tags = comment.get("message_tags")
    if tags and isinstance(tags, list):
        tags_str = json.dumps(tags, ensure_ascii=False)
    else:
        tags_str = ""

    return {
        "page_id": page_id,
        "page_name": page_name,
        "post_id": post_id,
        "post_created_time": post_created_time,
        "post_permalink": post_permalink,
        "comment_id": comment.get("id", ""),
        "parent_comment_id": parent_comment_id or "",
        "depth_level": depth_level,
        "created_time": comment.get("created_time", ""),
        "message": comment.get("message", ""),
        "commenter_id": commenter_id,
        "commenter_name": commenter_name,
        "commenter_profile_link": commenter_link,
        "like_count": comment.get("like_count", 0),
        "reaction_count": reaction_count,
        "reply_count": comment.get("comment_count", 0),
        "is_hidden": comment.get("is_hidden", False),
        "attachment_type": attachment_type,
        "message_tags": tags_str,
    }


def _deduplicate(comment_id: str) -> bool:
    """Return True if comment_id is new (not seen before)."""
    with seen_lock:
        if comment_id in seen_comment_ids:
            return False
        seen_comment_ids.add(comment_id)
        return True


# ---------------------------------------------------------------------------
# Recursive comment fetcher
# ---------------------------------------------------------------------------

def fetch_comments_for_post(
    session: requests.Session,
    writers: StreamWriters,
    page_id: str,
    page_name: str,
    post: dict[str, Any],
    delay: float,
) -> int:
    """Fetch all comments (including nested replies) for a single post. Returns count."""
    post_id = post["id"]
    post_created_time = post.get("created_time", "")
    post_permalink = post.get("permalink_url", "")
    total = 0

    def _fetch_level(
        parent_id: str,
        parent_comment_id: Optional[str],
        depth: int,
    ) -> None:
        nonlocal total
        if shutdown_event.is_set():
            return

        url = f"{GRAPH_API_BASE}/{parent_id}/comments"
        params = {"fields": COMMENT_FIELDS, "limit": DEFAULT_LIMIT}
        comments = paginate_all(session, url, params=params, delay=delay)

        for comment in comments:
            if shutdown_event.is_set():
                return
            cid = comment.get("id", "")
            if not cid or not _deduplicate(cid):
                continue

            record = _extract_comment_record(
                comment,
                page_id=page_id,
                page_name=page_name,
                post_id=post_id,
                post_created_time=post_created_time,
                post_permalink=post_permalink,
                parent_comment_id=parent_comment_id,
                depth_level=depth,
            )
            writers.write(record)
            total += 1

            if comment.get("comment_count", 0) > 0:
                _fetch_level(cid, cid, depth + 1)

    _fetch_level(post_id, None, 0)
    return total


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

def validate_token(session: requests.Session, token: str) -> None:
    url = f"{GRAPH_API_BASE}/me"
    try:
        resp = api_get(session, url, params={"access_token": token})
        uid = resp.get("id")
        if not uid:
            raise ValueError("Token validation returned no user id")
        logger.info("Token validated – user id: %s", uid)
    except Exception as exc:
        logger.error("Token validation failed: %s", exc)
        sys.exit(1)


def resolve_pages(
    session: requests.Session,
    token: str,
    page_ids: Optional[list[str]],
) -> list[dict[str, str]]:
    if page_ids:
        pages = []
        for pid in page_ids:
            url = f"{GRAPH_API_BASE}/{pid}"
            try:
                data = api_get(session, url, params={"fields": "id,name", "access_token": token})
                pages.append({"id": data["id"], "name": data.get("name", "")})
            except Exception as exc:
                logger.error("Cannot access page %s: %s", pid, exc)
        if not pages:
            logger.error("No accessible pages – exiting")
            sys.exit(1)
        return pages

    url = f"{GRAPH_API_BASE}/me/accounts"
    params = {"fields": "id,name", "limit": DEFAULT_LIMIT, "access_token": token}
    pages_raw = paginate_all(session, url, params=params)
    pages = [{"id": p["id"], "name": p.get("name", "")} for p in pages_raw]
    if not pages:
        logger.error("No pages found for this token – exiting")
        sys.exit(1)
    logger.info("Resolved %d page(s)", len(pages))
    return pages


def fetch_posts_for_page(
    session: requests.Session,
    token: str,
    page_id: str,
    since: str,
    until: str,
    delay: float,
) -> list[dict[str, Any]]:
    url = f"{GRAPH_API_BASE}/{page_id}/posts"
    params = {
        "fields": POST_FIELDS,
        "since": since,
        "until": until,
        "limit": DEFAULT_LIMIT,
        "access_token": token,
    }
    posts = paginate_all(session, url, params=params, delay=delay)
    logger.info("Page %s: fetched %d posts", page_id, len(posts))
    return posts


def process_page(
    session: requests.Session,
    token: str,
    writers: StreamWriters,
    page: dict[str, str],
    since: str,
    until: str,
    delay: float,
    max_workers: int,
    checkpoint_path: str,
) -> None:
    page_id = page["id"]
    page_name = page["name"]
    logger.info("Processing page: %s (%s)", page_name, page_id)

    checkpoint = load_checkpoint(checkpoint_path)
    completed_posts: set[str] = set(checkpoint.get("completed_posts", {}).get(page_id, []))

    posts = fetch_posts_for_page(session, token, page_id, since, until, delay)

    def _handle_post(post: dict[str, Any]) -> int:
        pid = post["id"]
        if pid in completed_posts:
            logger.debug("Skipping already-processed post %s", pid)
            return 0
        count = fetch_comments_for_post(
            session, writers, page_id, page_name, post, delay,
        )
        logger.info("Post %s: %d comments", pid, count)
        return count

    total_comments = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_post = {
            executor.submit(_handle_post, post): post for post in posts
        }
        for future in as_completed(future_to_post):
            if shutdown_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                break
            post = future_to_post[future]
            try:
                n = future.result()
                total_comments += n
                # Update checkpoint
                cp = load_checkpoint(checkpoint_path)
                cp.setdefault("completed_posts", {}).setdefault(page_id, [])
                if post["id"] not in cp["completed_posts"][page_id]:
                    cp["completed_posts"][page_id].append(post["id"])
                cp["last_page_id"] = page_id
                cp["last_post_id"] = post["id"]
                save_checkpoint(checkpoint_path, cp)
            except Exception:
                logger.exception("Error processing post %s", post.get("id"))

    writers.flush()
    logger.info(
        "Page %s done – %d comments from %d posts", page_id, total_comments, len(posts),
    )


# ---------------------------------------------------------------------------
# Date validation
# ---------------------------------------------------------------------------

def parse_date(value: str) -> str:
    """Validate ISO-8601 date string and return it."""
    try:
        datetime.fromisoformat(value)
    except ValueError:
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Invalid date format: {value!r} (expected ISO-8601, e.g. 2024-01-15)"
            )
    return value


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download all comments from Facebook Pages via Meta Graph API.",
    )
    parser.add_argument(
        "--access-token",
        default=os.getenv("FB_ACCESS_TOKEN", ""),
        help="Meta Graph API access token (or set FB_ACCESS_TOKEN env var / .env)",
    )
    parser.add_argument(
        "--page-ids",
        nargs="*",
        default=None,
        help="Specific page IDs to process (default: all accessible pages)",
    )
    parser.add_argument(
        "--since",
        required=True,
        type=parse_date,
        help="Start date (ISO-8601, e.g. 2024-01-01)",
    )
    parser.add_argument(
        "--until",
        required=True,
        type=parse_date,
        help="End date (ISO-8601, e.g. 2024-12-31)",
    )
    parser.add_argument(
        "--output-ndjson",
        default="comments.ndjson",
        help="Output NDJSON file path (default: comments.ndjson)",
    )
    parser.add_argument(
        "--output-csv",
        default="comments.csv",
        help="Output CSV file path (default: comments.csv)",
    )
    parser.add_argument(
        "--checkpoint",
        default=CHECKPOINT_FILE,
        help="Checkpoint file path (default: checkpoint.json)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay in seconds between API calls (default: 0)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Max parallel workers per page (default: 5)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    token = args.access_token
    if not token:
        logger.error("No access token provided. Use --access-token or set FB_ACCESS_TOKEN.")
        sys.exit(1)

    since = args.since
    until = args.until
    if since > until:
        logger.error("since_date (%s) must be <= until_date (%s)", since, until)
        sys.exit(1)

    session = requests.Session()
    session.params = {"access_token": token}  # type: ignore[assignment]

    validate_token(session, token)

    pages = resolve_pages(session, token, args.page_ids)
    logger.info("Pages to process: %s", [p["id"] for p in pages])

    writers = StreamWriters(args.output_ndjson, args.output_csv)

    try:
        for page in pages:
            if shutdown_event.is_set():
                break
            try:
                process_page(
                    session=session,
                    token=token,
                    writers=writers,
                    page=page,
                    since=since,
                    until=until,
                    delay=args.delay,
                    max_workers=args.max_workers,
                    checkpoint_path=args.checkpoint,
                )
            except requests.exceptions.HTTPError as exc:
                logger.error(
                    "Skipping page %s (%s) – API error: %s",
                    page.get("name", "?"), page["id"], exc,
                )
    except SystemExit:
        logger.info("Shutting down …")
    finally:
        writers.close()

    logger.info("Done – total API requests: %d", request_counter)


if __name__ == "__main__":
    main()
