import logging
import re
import tempfile
from collections import defaultdict
from pathlib import Path

import pdfplumber
from flows_staging.shared.config import get_config
from flows_staging.shared.download import write_csv_for_staging
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.models import StageConfig
from flows_staging.shared.staging_base import _process_single_file


logger = logging.getLogger(__name__)

FIELDNAMES = ["commune", "dept_code", "nb_lauriers"]
EXTENSION = ".csv"


SECTION_RE = re.compile(r"\d+\s+villes?\s+\u201c(\d+)\s+LAURIERS?\u201d", re.IGNORECASE)
ENTRY_RE = re.compile(r"^(.+?)\s*\((\d{1,3}[AB]?)\)$")
JUNK_RE = re.compile(
    r"(Palmar|pouvez|labelli|depuis|internet|www\.|^Ville$|Active|Sportive|\d{4})",
    re.IGNORECASE,
)

COL_BREAKS = (190, 370)


def col_index(x: float) -> int:
    if x < COL_BREAKS[0]:
        return 0
    if x < COL_BREAKS[1]:
        return 1
    return 2


def _flush(col: int, words: list, col_tokens: dict, page: int) -> None:
    if not words:
        return
    text = " ".join(w["text"] for w in words)
    if JUNK_RE.search(text):
        return
    sec = SECTION_RE.search(text)
    if sec:
        col_tokens[(page, col)].append(("section", int(sec.group(1))))
        return
    m = ENTRY_RE.match(text.strip())
    if m:
        commune = m.group(1).strip().lower()
        dept = m.group(2).zfill(2)
        col_tokens[(page, col)].append(("entry", commune, dept))


def extract_col_tokens(path: Path) -> dict:
    col_tokens: dict = defaultdict(list)
    with pdfplumber.open(path) as pdf:
        for pnum, page in enumerate(pdf.pages):
            by_y: dict = defaultdict(list)
            for w in page.extract_words():
                by_y[round(w["top"])].append(w)
            for _y, ws in sorted(by_y.items()):
                ws.sort(key=lambda w: w["x0"])
                current_col: int | None = None
                chunk: list = []
                for w in ws:
                    c = col_index(w["x0"])
                    if c != current_col:
                        if current_col is not None:
                            _flush(current_col, chunk, col_tokens, pnum)
                        current_col, chunk = c, []
                    chunk.append(w)
                if current_col is not None:
                    _flush(current_col, chunk, col_tokens, pnum)
    return col_tokens


def parse_palmares(path: Path) -> list[dict]:
    col_tokens = extract_col_tokens(path)
    results: list[dict] = []
    current_lauriers: int | None = None
    for key in sorted(col_tokens.keys()):
        for tok in col_tokens[key]:
            if tok[0] == "section":
                current_lauriers = tok[1]
            elif tok[0] == "entry" and current_lauriers is not None:
                results.append(
                    {
                        "commune": tok[1],
                        "dept_code": tok[2],
                        "nb_lauriers": current_lauriers,
                    }
                )
    return results


def run(config: dict, run_id: str) -> bool:
    """Parse ville sportive PDF and stage via the shared pipeline.

    Parses the PDF, writes output to a temporary CSV, then hands off to
    `_process_single_file` which handles MD5 comparison, archiving the old
    version, uploading, and writing metadata.

    Args:
        config: Full config dict (from config.yaml custom_parsers section).
        run_id: Unique flow run identifier.

    Returns:
        True if a file was staged, False if skipped or failed.
    """
    parser_config = next(
        s
        for s in config["custom_parsers"]
        if s["module"] == "flows_staging.custom_parsers.parse_ville_sportive"
    )

    input_dir = Path(parser_config.get("input_dir", "custom_parsers/data_for_parsers"))
    pdf_path = input_dir / parser_config["pdf_file"]

    logger.info("Parsing %s...", pdf_path)
    rows = parse_palmares(pdf_path)

    counts = {k: sum(1 for r in rows if r["nb_lauriers"] == k) for k in [1, 2, 3, 4]}
    logger.info("  1 laurier : %d", counts[1])
    logger.info("  2 lauriers: %d", counts[2])
    logger.info("  3 lauriers: %d", counts[3])
    logger.info("  4 lauriers: %d", counts[4])
    logger.info("  Total     : %d", len(rows))

    if not rows:
        logger.warning("No data parsed")
        return False

    all_config = get_config()
    staging_bucket = all_config["buckets"]["staging_current"]
    evidence_bucket = all_config["buckets"]["evidence_archive"]
    minio_client = get_minio_client()

    stage_config = StageConfig(
        name=parser_config["name"],
        url="",  # No source URL for PDF parsers
        target_folder=parser_config.get("target_folder", "labels"),
        run_id=run_id,
        staging_bucket=staging_bucket,
        evidence_bucket=evidence_bucket,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        write_csv_for_staging(rows, FIELDNAMES, parser_config["name"], temp_path)
        return _process_single_file(
            stage_config, minio_client, parser_config["name"], EXTENSION, temp_path
        )
