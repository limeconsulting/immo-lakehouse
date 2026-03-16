#!/usr/bin/env python3
import argparse
import csv
import gzip
import hashlib
import os
import sys
import tempfile
import time
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# Requires: pip install minio
from minio import Minio
from minio.error import S3Error

# Default (geo-dvf "latest" layout you've been using)
DEFAULT_URL_TEMPLATES = [
    "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/full.csv.gz",
    # Fallbacks if "latest" layout changes in the future (keep pipeline stable)
    "https://files.data.gouv.fr/geo-dvf/csv/{year}/full.csv.gz",
]

USER_AGENT = "immo-lake/1.1"

# Candidates for department column name across variants
DEP_COL_CANDIDATES = [
    "Code departement",
    "code_departement",
    "code département",
    "code_departement_libelle",  # rare, but harmless
]


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download_with_retries(url: str, out_path: str, timeout: int = 90, retries: int = 3, backoff_s: int = 2) -> None:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=timeout) as r, open(out_path, "wb") as f:
                while True:
                    b = r.read(1024 * 1024)
                    if not b:
                        break
                    f.write(b)
            return
        except (HTTPError, URLError, TimeoutError) as e:
            last_err = e
            if attempt < retries:
                sleep_s = backoff_s * attempt
                print(f"[WARN] Download failed (attempt {attempt}/{retries}) {e}. Retrying in {sleep_s}s...", file=sys.stderr)
                time.sleep(sleep_s)
            else:
                raise RuntimeError(f"Download failed after {retries} attempts: {url} ({e})") from e
    if last_err:
        raise RuntimeError(f"Download failed: {url} ({last_err})")


def resolve_dvf_url(year: int, explicit_template: Optional[str], timeout: int, retries: int) -> str:
    templates = [explicit_template] if explicit_template else []
    templates += DEFAULT_URL_TEMPLATES

    errors = []
    for tpl in templates:
        if not tpl:
            continue
        url = tpl.format(year=year)
        # quick probe: attempt small open (we'll download fully after)
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=timeout) as r:
                # if reachable, great (don’t read full)
                _ = r.read(1)
            return url
        except Exception as e:
            errors.append((url, str(e)))
            continue

    msg = "Could not resolve a working DVF URL. Tried:\n" + "\n".join([f"- {u} :: {err}" for u, err in errors])
    raise RuntimeError(msg)


def parse_years(year: Optional[int], years: Optional[str]) -> List[int]:
    if year is not None and years is not None:
        raise ValueError("Use either --year or --years (range), not both.")
    if year is not None:
        return [year]
    if not years:
        raise ValueError("Missing --year or --years.")
    s = years.strip()
    # formats accepted: "2020-2025" or "2020,2021,2022"
    if "-" in s and "," not in s:
        a, b = s.split("-", 1)
        y0, y1 = int(a), int(b)
        if y1 < y0:
            raise ValueError("--years range must be ascending, e.g. 2020-2025")
        return list(range(y0, y1 + 1))
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return [int(p) for p in parts]


def filter_dep_from_gz_csv(src_gz: str, dep: str, out_gz: str) -> Dict[str, int]:
    """
    Keep only rows with department == dep.

    IMPORTANT: uses csv.reader to properly handle quoted fields containing commas.
    Works for geo-dvf full.csv.gz where delimiter is comma.
    """
    kept = 0
    total = 0

    with gzip.open(src_gz, "rt", encoding="utf-8", errors="replace", newline="") as fin, \
         gzip.open(out_gz, "wt", encoding="utf-8", newline="") as fout:

        reader = csv.reader(fin, delimiter=",", quotechar='"')
        writer = csv.writer(fout, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

        try:
            header = next(reader)
        except StopIteration:
            raise RuntimeError("Empty DVF file.")

        writer.writerow(header)

        dep_idx = None
        header_map = {name: i for i, name in enumerate(header)}
        for cand in DEP_COL_CANDIDATES:
            if cand in header_map:
                dep_idx = header_map[cand]
                break
        if dep_idx is None:
            raise RuntimeError(
                "Department column not found. Tried: "
                + ", ".join(DEP_COL_CANDIDATES)
                + f". Header columns sample: {header[:40]}"
            )

        for row in reader:
            total += 1
            if dep_idx >= len(row):
                continue
            # normalize value (strip quotes already handled by csv)
            v = (row[dep_idx] or "").strip()
            if v == dep:
                writer.writerow(row)
                kept += 1

    return {"total_rows": total, "kept_rows": kept}


def mk_minio_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    secure = endpoint.startswith("https://")
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(host, access_key=access_key, secret_key=secret_key, secure=secure)


def put_text_object(mc: Minio, bucket: str, key: str, text: str) -> None:
    import io
    data = text.encode("utf-8")
    mc.put_object(bucket, key, io.BytesIO(data), length=len(data), content_type="text/plain; charset=utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, help="Single year (alternative to --years).")
    ap.add_argument("--years", type=str, help='Range/list, e.g. "2020-2025" or "2020,2021,2022".')
    ap.add_argument("--dep", required=True, type=str)
    ap.add_argument("--ingest-id", type=str, default=None, help="Stable ingest id shared across years.")
    ap.add_argument("--minio-endpoint", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--url-template", type=str, default=None, help="Override DVF URL template, must include {year}.")
    ap.add_argument("--timeout", type=int, default=90)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    dep = args.dep.strip()
    years_list = parse_years(args.year, args.years)

    ingest_id = args.ingest_id or datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")

    mc = mk_minio_client(args.minio_endpoint, args.access_key, args.secret_key)

    try:
        if not mc.bucket_exists(args.bucket):
            raise RuntimeError(f"Bucket '{args.bucket}' does not exist. Create/init it first (mc mb ...).")
    except S3Error as e:
        print(f"MinIO error during bucket check: {e}", file=sys.stderr)
        sys.exit(2)

    os.makedirs("/tmp/immo-lake", exist_ok=True)

    for year in years_list:
        print(f"\n=== INGEST year={year} dep={dep} ingest_id={ingest_id} ===")

        url = resolve_dvf_url(year, args.url_template, timeout=args.timeout, retries=args.retries)
        print(f"Resolved DVF URL: {url}")

        with tempfile.TemporaryDirectory(prefix="immo-lake-") as td:
            raw_path = os.path.join(td, f"dvf_full_{year}.csv.gz")
            dep_path = os.path.join(td, f"dvf_dep{dep}_{year}.csv.gz")

            print(f"Downloading DVF: {url}")
            if not args.dry_run:
                download_with_retries(url, raw_path, timeout=args.timeout, retries=args.retries)
                raw_sha = sha256_file(raw_path)
                raw_size = os.path.getsize(raw_path)
                print(f"Downloaded: {raw_path} size={raw_size} sha256={raw_sha}")
            else:
                raw_sha = "DRY_RUN"
                raw_size = -1
                print("[DRY_RUN] skip download")

            print(f"Filtering department={dep} ...")
            if not args.dry_run:
                stats = filter_dep_from_gz_csv(raw_path, dep, dep_path)
                dep_sha = sha256_file(dep_path)
                dep_size = os.path.getsize(dep_path)
                print(f"Filtered rows kept={stats['kept_rows']} total_scanned={stats['total_rows']}")
                print(f"Output: {dep_path} size={dep_size} sha256={dep_sha}")
            else:
                stats = {"total_rows": 0, "kept_rows": 0}
                dep_sha = "DRY_RUN"
                dep_size = -1
                print("[DRY_RUN] skip filter")

            key = f"raw/dvf/ingest={ingest_id}/year={year}/dep={dep}/dvf_dep{dep}_{year}.csv.gz"
            manifest_key = f"raw/dvf/ingest={ingest_id}/year={year}/dep={dep}/manifest.txt"

            manifest = (
                f"url={url}\n"
                f"year={year}\n"
                f"dep={dep}\n"
                f"ingest_id={ingest_id}\n"
                f"raw_size_bytes={raw_size}\n"
                f"raw_sha256={raw_sha}\n"
                f"dep_size_bytes={dep_size}\n"
                f"dep_sha256={dep_sha}\n"
                f"total_rows_scanned={stats.get('total_rows', 0)}\n"
                f"kept_rows={stats.get('kept_rows', 0)}\n"
                f"generated_utc={datetime.utcnow().strftime('%Y-%m-%dT%H%M%SZ')}\n"
            )

            if args.dry_run:
                print(f"[DRY_RUN] Would upload to s3://{args.bucket}/{key}")
                print(f"[DRY_RUN] Would upload manifest s3://{args.bucket}/{manifest_key}")
                continue

            try:
                mc.fput_object(args.bucket, key, dep_path, content_type="application/gzip")
                put_text_object(mc, args.bucket, manifest_key, manifest)
                print(f"Uploaded to s3://{args.bucket}/{key}")
                print(f"Uploaded manifest s3://{args.bucket}/{manifest_key}")
            except S3Error as e:
                print(f"MinIO error during upload: {e}", file=sys.stderr)
                sys.exit(2)

    print("\nDONE.")


if __name__ == "__main__":
    main()
