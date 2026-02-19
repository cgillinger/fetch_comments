# Facebook Page Comments Downloader

Downloads all comments (including nested replies) from Facebook Page posts using the Meta Graph API v22.0.

## Requirements

- Python 3.11+
- A Meta (Facebook) access token with the permissions:
  - `pages_read_engagement`
  - `pages_read_user_content`

## Installation

```bash
pip install -r requirements.txt
```

## Access Token

You need a valid Meta Graph API token. There are three ways to provide it:

**1. Command-line argument**

```bash
python fetch_comments.py --access-token "YOUR_TOKEN" --since 2024-01-01 --until 2024-12-31
```

**2. Environment variable**

```bash
export FB_ACCESS_TOKEN="YOUR_TOKEN"
python fetch_comments.py --since 2024-01-01 --until 2024-12-31
```

**3. `.env` file** (in the same directory as the script)

```
FB_ACCESS_TOKEN=YOUR_TOKEN
```

```bash
python fetch_comments.py --since 2024-01-01 --until 2024-12-31
```

Priority order: `--access-token` > environment variable > `.env` file.

### How to get a token

1. Go to [Meta for Developers](https://developers.facebook.com/) and create an app.
2. In the app dashboard, add the **Facebook Login** product.
3. Use the [Graph API Explorer](https://developers.facebook.com/tools/explorer/) to generate a token with the required permissions (`pages_read_engagement`, `pages_read_user_content`).
4. For long-lived tokens, exchange your short-lived token via the [token exchange endpoint](https://developers.facebook.com/docs/facebook-login/guides/access-tokens/get-long-lived/).

## Usage

### Required arguments

| Argument | Description |
|---|---|
| `--since` | Start date in ISO-8601 format (e.g. `2024-01-01`) |
| `--until` | End date in ISO-8601 format (e.g. `2024-12-31`) |

These dates filter by **post publication date**, not comment creation date. All comments on matching posts are downloaded regardless of when the comments were written.

### Optional arguments

| Argument | Default | Description |
|---|---|---|
| `--access-token` | from env/`.env` | Meta Graph API access token |
| `--page-ids` | all accessible pages | Space-separated list of specific Page IDs to process |
| `--week` | off | Split output into weekly files (ISO weeks, Mon–Sun) |
| `--clean` | off | Also produce token-efficient `*_clean` files for LLM analysis |
| `--output-ndjson` | auto-generated | Override NDJSON path |
| `--csv` | off | Also produce CSV output |
| `--output-csv` | auto-generated | Override CSV path (implies `--csv`) |
| `--checkpoint` | `checkpoint.json` | Checkpoint file for resume support |
| `--delay` | `0.0` | Delay in seconds between API calls |
| `--max-workers` | `5` | Max parallel threads per page |

### Examples

Download all comments from all accessible pages for 2024:

```bash
python fetch_comments.py --since 2024-01-01 --until 2024-12-31
```

Download from specific pages only:

```bash
python fetch_comments.py \
  --page-ids 123456789 987654321 \
  --since 2024-06-01 \
  --until 2024-06-30
```

With a conservative delay to avoid rate limits:

```bash
python fetch_comments.py \
  --since 2024-01-01 \
  --until 2024-12-31 \
  --delay 0.5 \
  --max-workers 3
```

Custom output file paths:

```bash
python fetch_comments.py \
  --since 2024-01-01 \
  --until 2024-12-31 \
  --output-ndjson data/output.ndjson \
  --output-csv data/output.csv
```

## Output

The script writes two output files simultaneously in streaming mode (safe for >1 million comments):

### NDJSON (`comments.ndjson`)

One JSON object per line:

```json
{"page_id": "123", "page_name": "My Page", "post_id": "123_456", "comment_id": "789", "message": "Great post!", ...}
```

### CSV (`comments.csv`)

Standard CSV with headers. Suitable for Excel, Google Sheets, pandas, etc.

### Output fields

| Field | Description |
|---|---|
| `page_id` | Facebook Page ID |
| `page_name` | Facebook Page name |
| `post_id` | Post ID the comment belongs to |
| `post_created_time` | When the post was published (ISO-8601) |
| `post_permalink` | Direct URL to the post |
| `comment_id` | Unique comment ID |
| `parent_comment_id` | Parent comment ID (empty for top-level comments) |
| `depth_level` | Nesting depth (`0` = top-level, `1` = reply, `2` = reply-to-reply, ...) |
| `created_time` | When the comment was posted (ISO-8601) |
| `message` | Comment text |
| `commenter_id` | Commenter's Facebook user/page ID |
| `commenter_name` | Commenter's display name |
| `commenter_profile_link` | Link to commenter's profile |
| `like_count` | Number of likes on the comment |
| `reaction_count` | Total reactions (all types) |
| `reply_count` | Number of direct replies to this comment |
| `is_hidden` | Whether the comment is hidden by the page admin |
| `attachment_type` | Type of attachment (photo, video, sticker, etc.) |
| `message_tags` | Tagged users/pages in the comment (JSON array) |

## Clean Mode (`--clean`)

Generates additional token-efficient export files optimized for LLM-based qualitative analysis. The regular (full) export is always produced — `--clean` adds `*_clean.ndjson` (and `*_clean.csv` if `--csv` is used) alongside the default output.

```bash
python fetch_comments.py --since 2025-11-01 --until 2025-11-07 --clean
```

This produces both `PageName_251101-251107_comments.ndjson` and `PageName_251101-251107_comments_clean.ndjson`.

### What changes in clean mode

**Kept fields:**

| Field | Notes |
|---|---|
| `created_time` | Comment timestamp |
| `post_id` | Groups comments by post |
| `depth_level` | `0` = top-level, `1`+ = reply |
| `is_hidden` | Hidden by page admin |
| `reaction_count` | Total reactions (all types) |
| `message` | Comment text |
| `post_message` | Truncated to 200 characters |

**Removed fields:** `page_id`, `page_name`, `post_created_time`, `post_permalink`, `comment_id`, `parent_comment_id`, `commenter_id`, `commenter_name`, `commenter_profile_link`, `like_count`, `reply_count`, `attachment_type`, `message_tags`.

`reply_count` is removed because reply structure can be derived from `depth_level` (depth > 0 = reply in a thread).

**Post message truncation:** `post_message` is truncated to 200 characters (with "…" appended) to reduce token usage while preserving enough context for analysis.

### Combining with other flags

```bash
# Clean + weekly split + CSV
python fetch_comments.py --since 2025-10-01 --until 2025-10-31 --week --clean --csv
```

## Resume / Checkpoint

The script saves progress to `checkpoint.json` after each completed post. If the script is interrupted (Ctrl+C, network failure, etc.), simply re-run the same command and it will skip already-processed posts.

To start fresh, delete the checkpoint file:

```bash
rm checkpoint.json
```

## Rate Limit Handling

The script handles Meta API rate limits automatically:

- **HTTP 429** and error codes **4**, **17**, **32** trigger retries.
- Uses the `Retry-After` header when provided, otherwise exponential backoff (2s, 4s, 8s, ... up to 120s).
- Max 8 retry attempts per request before aborting.
- Monitors `X-App-Usage` and `X-Page-Usage` headers. If any metric exceeds 80%, inserts a random 5-15 second pause.

For large-scale runs, use `--delay` and reduce `--max-workers` to stay well within limits.

## Logging

All activity is logged to both:

- **stderr** (console) -- real-time progress
- **`fetch_comments.log`** -- persistent log file

Log entries include timestamps, request counts, rate-limit events, and errors.

## Graceful Shutdown

Press **Ctrl+C** at any time. The script will:

1. Stop accepting new work.
2. Wait for in-flight requests to finish.
3. Flush and close output files.
4. Save the checkpoint.

You can safely resume afterwards.

## Project Structure

```
fetch_comments/
  fetch_comments.py    # Main script
  requirements.txt     # Python dependencies
  .env                 # Your access token (not committed)
  comments.ndjson      # Output (generated)
  comments.csv         # Output (generated)
  checkpoint.json      # Resume state (generated)
  fetch_comments.log   # Log file (generated)
```
