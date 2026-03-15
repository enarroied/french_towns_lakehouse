import csv
import re
from collections import defaultdict
from pathlib import Path

import pdfplumber
import yaml


def load_config() -> dict:
    with open("config.yaml") as f:
        return yaml.safe_load(f)


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
                        _flush(current_col, chunk, col_tokens, pnum)
                        current_col, chunk = c, []
                    chunk.append(w)
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


def run(config: dict) -> Path:
    parser_config = next(
        s
        for s in config["custom_parsers"]
        if s["module"] == "custom_parsers.parse_ville_sportive"
    )
    output_dir = Path(config["paths"]["custom_dir"])
    output_dir.mkdir(exist_ok=True, parents=True)
    output_path = output_dir / f"{parser_config['name']}.csv"

    input_dir = Path(parser_config.get("input_dir", "custom_parsers/data_for_parsers"))
    pdf_path = input_dir / parser_config["pdf_file"]

    print(f"Parsing {pdf_path}...")
    rows = parse_palmares(pdf_path)

    counts = {k: sum(1 for r in rows if r["nb_lauriers"] == k) for k in [1, 2, 3, 4]}
    print(f"  1 laurier : {counts[1]}")
    print(f"  2 lauriers: {counts[2]}")
    print(f"  3 lauriers: {counts[3]}")
    print(f"  4 lauriers: {counts[4]}")
    print(f"  Total     : {len(rows)}")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["commune", "dept_code", "nb_lauriers"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved → {output_path}")
    return output_path


if __name__ == "__main__":
    config = load_config()
    run(config)
