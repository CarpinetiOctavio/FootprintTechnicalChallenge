import csv
import re
import sys
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Hashable

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
UNKNOWN_DATE  = "Unknown"
UNKNOWN_TITLE = "Untitled Episode"
UNKNOWN_SORT  = 999  # Sentinel: unknown season/episode sorts to end

DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%B %d, %Y",
    "%b %d, %Y",
]

REQUIRED_COLUMNS = {
    "SeriesName",
    "SeasonNumber",
    "EpisodeNumber",
    "EpisodeTitle",
    "AirDate",
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class Episode:
    series_name:    str
    season_number:  int
    episode_number: int
    episode_title:  str
    air_date:       str
    original_index: int
    corrected:      bool = False

    @property
    def series_norm(self) -> str:
        return normalize_string(self.series_name)

    @property
    def title_norm(self) -> str:
        return normalize_string(self.episode_title)


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------
def normalize_string(s: str) -> str:
    return re.sub(r'\s+', ' ', s.strip()).lower()


def clean_string(s: str) -> str:
    return re.sub(r'\s+', ' ', s.strip())


def parse_number(s: str) -> Optional[int]:
    s = s.strip()
    if not s:
        return None
    try:
        val = int(s)
        return val if val >= 0 else None
    except ValueError:
        return None


def parse_date(s: str) -> Optional[str]:
    s = s.strip()
    if not s:
        return None
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            # Sanity bound: covers any plausible television/streaming era
            if dt.year < 1 or dt.year > 2100:
                return None
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def is_corrected(raw: str, final) -> bool:
    return raw.strip() != str(final)


# ---------------------------------------------------------------------------
# Identity check
# ---------------------------------------------------------------------------
def has_identity(ep: Episode) -> bool:
    return not (
        ep.episode_number == 0
        and ep.episode_title == UNKNOWN_TITLE
        and ep.air_date == UNKNOWN_DATE
    )


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
def compute_key(ep: Episode) -> tuple[Hashable, ...]:
    sn = ep.series_norm
    s  = ep.season_number
    e  = ep.episode_number
    t  = ep.title_norm

    if s != 0 and e != 0:
        return (sn, s, e)
    elif s == 0 and e != 0:
        return (sn, 0, e, t)
    elif s != 0 and e == 0:
        return (sn, s, 0, t)
    else:
        # Edge case: both unknown. Group by title as best available identifier.
        # Design decision: extends spec pattern consistently. Documented in report.
        return (sn, 0, 0, t)


def score_record(ep: Episode) -> int:
    score = 0
    if ep.air_date != UNKNOWN_DATE:
        score += 4
    if ep.episode_title != UNKNOWN_TITLE:
        score += 2
    if ep.season_number and ep.episode_number:
        score += 1
    return score


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def process_csv(input_path: Path):
    stats = {
        "total_input":           0,
        "discarded_no_series":   0,
        "discarded_no_identity": 0,
        "corrected":             0,
        "duplicates_removed":    0,
        "total_output":          0,
    }

    validated = []

    # Only the file open is guarded: encoding/permission errors get a clear
    # message; internal bugs surface as full tracebacks for easier debugging.
    try:
        f = open(input_path, newline='', encoding='utf-8')
    except OSError as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    with f:
        reader = csv.DictReader(f)

        if not REQUIRED_COLUMNS.issubset(set(reader.fieldnames or [])):
            missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
            missing_str = ", ".join(sorted(missing))
            print(f"Error: CSV missing required columns: {missing_str}")
            sys.exit(1)

        # enumerate starts at 2: line 1 is the header, first data row is line 2
        for idx, row in enumerate(reader, start=2):
            stats["total_input"] += 1

            raw_series  = row.get("SeriesName")    or ""
            raw_season  = row.get("SeasonNumber")  or ""
            raw_episode = row.get("EpisodeNumber") or ""
            raw_title   = row.get("EpisodeTitle")  or ""
            raw_date    = row.get("AirDate")       or ""

            series_name = clean_string(raw_series)
            if not series_name:
                stats["discarded_no_series"] += 1
                continue

            parsed_season  = parse_number(raw_season)
            season_number  = parsed_season if parsed_season is not None else 0

            parsed_episode = parse_number(raw_episode)
            episode_number = parsed_episode if parsed_episode is not None else 0

            episode_title  = clean_string(raw_title) or UNKNOWN_TITLE

            parsed_date    = parse_date(raw_date)
            air_date       = parsed_date if parsed_date is not None else UNKNOWN_DATE

            corrected = any([
                is_corrected(raw_series,  series_name),
                is_corrected(raw_season,  season_number),
                is_corrected(raw_episode, episode_number),
                is_corrected(raw_title,   episode_title),
                is_corrected(raw_date,    air_date),
            ])

            ep = Episode(
                series_name=series_name,
                season_number=season_number,
                episode_number=episode_number,
                episode_title=episode_title,
                air_date=air_date,
                original_index=idx,
                corrected=corrected,
            )

            if not has_identity(ep):
                stats["discarded_no_identity"] += 1
                continue

            validated.append(ep)

    groups = defaultdict(list)
    for ep in validated:
        groups[compute_key(ep)].append(ep)

    output = []
    for group in groups.values():
        best = max(group, key=lambda e: (score_record(e), -e.original_index))
        output.append(best)
        stats["duplicates_removed"] += len(group) - 1

    # Count corrections present in the final output records only
    stats["corrected"] = sum(1 for ep in output if ep.corrected)

    output.sort(key=lambda e: (
        e.series_norm,
        e.season_number  or UNKNOWN_SORT,
        e.episode_number or UNKNOWN_SORT,
    ))

    stats["total_output"] = len(output)
    return output, stats


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------
def write_clean_csv(output: list, path: Path):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["SeriesName", "SeasonNumber", "EpisodeNumber",
                         "EpisodeTitle", "AirDate"])
        for ep in output:
            writer.writerow([
                ep.series_name,
                ep.season_number,
                ep.episode_number,
                ep.episode_title,
                ep.air_date,
            ])


def write_report(stats: dict, path: Path):
    ti  = stats["total_input"]
    to  = stats["total_output"]
    dns = stats["discarded_no_series"]
    dni = stats["discarded_no_identity"]
    cor = stats["corrected"]
    dup = stats["duplicates_removed"]

    check_ok  = (ti == to + dns + dni + dup)
    check_str = f"{ti} = {to} + {dns} + {dni} + {dup} → {'✓' if check_ok else '✗ MISMATCH'}"

    content = f"""# Data Quality Report

## Summary

| Metric                              | Count |
|-------------------------------------|-------|
| Total input records                 | {ti}  |
| Total output records                | {to}  |
| Discarded — missing series name     | {dns} |
| Discarded — insufficient identity   | {dni} |
| Corrected entries (in output)       | {cor} |
| Duplicates removed                  | {dup} |

**Integrity check:** `{check_str}`

---

## Deduplication Strategy

Episodes are identified by a **canonical key** computed after normalization
(trimmed, collapsed spaces, lowercased — for comparison only; output preserves
original casing from the best record).

The key type depends on which fields are known (non-zero / non-default):

| Season | Episode | Key used |
|--------|---------|----------|
| ≠ 0    | ≠ 0     | `(series_norm, season, episode)` |
| = 0    | ≠ 0     | `(series_norm, 0, episode, title_norm)` |
| ≠ 0    | = 0     | `(series_norm, season, 0, title_norm)` |
| = 0    | = 0     | `(series_norm, 0, 0, title_norm)` *(edge case — see note below)* |

When multiple records share the same key, the **best record** is selected
using a scoring system that mirrors the spec's priority order:

| Criterion                              | Points |
|----------------------------------------|--------|
| Valid Air Date (not `"Unknown"`)       | 4      |
| Known Title (not `"Untitled Episode"`) | 2      |
| Both Season and Episode non-zero       | 1      |
| Tiebreaker                             | First occurrence in file |

> **Note on AirDate in the key:** AirDate is intentionally excluded from all
> deduplication keys. Including it would prevent merging two records that
> represent the same episode — one with a known date, one without — which is
> a common real-world scenario. AirDate is used exclusively as a scoring
> criterion.

> **Edge case (Season = 0, Episode = 0):** The spec does not define a key
> for this combination. The chosen key `(series_norm, 0, 0, title_norm)`
> extends the spec's pattern consistently. Records with identical normalized
> title in the same series, both with unknown season and episode, are treated
> as duplicates.
"""

    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python episode_pipeline.py <input.csv>")
        sys.exit(1)

    input_path    = Path(sys.argv[1])
    output_csv    = Path("episodes_clean.csv")
    output_report = Path("report.md")

    if not input_path.exists():
        print(f"Error: file not found: {input_path}")
        sys.exit(1)

    print(f"Processing: {input_path}")
    output, stats = process_csv(input_path)

    write_clean_csv(output, output_csv)
    write_report(stats, output_report)

    print(f"\nDone.")
    print(f"  Input records     : {stats['total_input']}")
    print(f"  Output records    : {stats['total_output']}")
    print(f"  Discarded         : {stats['discarded_no_series'] + stats['discarded_no_identity']}")
    print(f"  Corrected         : {stats['corrected']}")
    print(f"  Duplicates removed: {stats['duplicates_removed']}")
    print(f"\nFiles written: {output_csv}, {output_report}")


if __name__ == "__main__":
    main()