# Facebook Page Comments Downloader

Downloads all comments (including nested replies) from Facebook Page posts using the Meta Graph API v22.0.

> This is a personal hobby project I build for my own use and publish in case it's useful to someone else. I work on it in my spare time, so issues and PRs are welcome but replies may be slow. Use at your own risk.

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
| `--month` | off | Split output into monthly files (calendar months) |
| `--clean` | off | Produce only a token-efficient clean CSV (no NDJSON) |
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

Split output into one file per calendar month:

```bash
python fetch_comments.py \
  --since 2024-09-01 \
  --until 2024-12-31 \
  --month
```

This produces separate files in monthly folders:

```
24-SEP/PageName_240901-240930_comments.csv
24-OKT/PageName_241001-241031_comments.csv
24-NOV/PageName_241101-241130_comments.csv
24-DEC/PageName_241201-241231_comments.csv
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

Generates a single token-efficient CSV optimised for LLM ingestion. **No NDJSON is produced** in clean mode. The default (full) export is unchanged when `--clean` is not used.

```bash
python fetch_comments.py --since 2025-11-01 --until 2025-11-07 --clean
```

Produces only:

```
26-NOV/PageName_251101-251107_comments_clean.csv
```

With a custom output path:

```bash
python fetch_comments.py --since 2025-11-01 --until 2025-11-07 --clean --output-csv data/output.csv
```

Produces: `data/output_clean.csv`

### Clean CSV schema (strict column order)

| # | Column | Type | Notes |
|---|---|---|---|
| 1 | `created_time_unix` | integer | ISO-8601 → Unix timestamp (seconds, UTC) |
| 2 | `post_id` | string | Groups comments by post |
| 3 | `depth_level` | integer | `0` = top-level, `1`+ = reply |
| 4 | `is_hidden` | 0/1 | `1` = hidden by page admin |
| 5 | `reaction_count` | integer | Total reactions (all types) |
| 6 | `reply_count` | integer | Number of direct replies to this comment |
| 7 | `message` | string | Sanitised comment text |

No other columns are included.

**Example row:**

```csv
1730620917,163892707053917_1026716302894344,0,0,12,3,"Här är det redan vitt på marken."
```

### Field transformations

- **`created_time`** is converted from ISO-8601 to a Unix timestamp (integer seconds). Parse failures produce an empty field and a logged warning.
- **`is_hidden`** is mapped to `1` (hidden) / `0` (visible).
- **`message`** is sanitised for stable CSV and LLM ingestion:
  - Leading/trailing whitespace stripped.
  - `\r\n`, `\r`, `\n` replaced with a single space.
  - Multiple consecutive whitespace collapsed into one space.
  - Null bytes (`\x00`) removed.
  - UTF-8 characters preserved; text is **not** truncated.

### Removed fields

`page_id`, `page_name`, `post_created_time`, `post_permalink`, `post_message`, `comment_id`, `parent_comment_id`, `commenter_id`, `commenter_name`, `commenter_profile_link`, `like_count`, `attachment_type`, `message_tags`.

`post_message` is excluded to reduce token count — it repeats per comment and the post context can be joined via `post_id` when needed.

### Combining with other flags

```bash
# Clean + weekly split
python fetch_comments.py --since 2025-10-01 --until 2025-10-31 --week --clean

# Clean + monthly split
python fetch_comments.py --since 2025-09-01 --until 2025-12-31 --month --clean
```

> **Note:** `--week` and `--month` are mutually exclusive — you can use one or the other, but not both.

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
