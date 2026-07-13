#!/usr/bin/env python3
"""Download, harmonize, and stage Filosofi income data (2013–2023).

Output: harmonized CSV written to /tmp, then uploaded to MinIO staging.
"""

import csv
import io
import math
import shutil
import tempfile
import zipfile
from pathlib import Path
from urllib.request import urlopen

import xlrd


RAW_DIR = Path("/tmp/filosofi_raw")

COLUMNS = [
    "id",
    "year",
    "methodology_version",
    "nb_tax_households",
    "nb_persons",
    "median_income",
    "poverty_rate",
    "decile1",
    "decile9",
    "gini",
    "s80_s20",
    "activity_income_share",
    "salary_share",
    "unemployment_share",
    "pension_share",
    "property_income_share",
    "social_benefits_share",
    "family_benefits_share",
    "minimum_social_share",
    "housing_benefits_share",
    "tax_share",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def download(url: str, dest: Path) -> None:
    if dest.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  DL {url.rsplit('/', 1)[-1]}")
    with urlopen(url) as resp, dest.open("wb") as f:
        shutil.copyfileobj(resp, f)


def read_from_zip(zip_path: Path, suffix: str) -> bytes:
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.filename.endswith(suffix):
                return zf.read(info)
    raise FileNotFoundError(f"{suffix} not in {zip_path}")


def pf(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, float):
        return val if not math.isnan(val) else None
    s = str(val).strip().strip('"').strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def make_row(year: int, method: str, id_code: str, **kw) -> dict:
    row = dict.fromkeys(COLUMNS)
    row["id"] = id_code
    row["year"] = year
    row["methodology_version"] = method
    for k, v in kw.items():
        if k in row:
            row[k] = pf(v)
    return row


# ---------------------------------------------------------------------------
# XLS reader — communes sheet, codes on row 5, data from row 6
# ---------------------------------------------------------------------------


def read_xls_codes(data: bytes, sheet: str = "ENSEMBLE") -> list[dict]:
    with tempfile.NamedTemporaryFile(suffix=".xls", delete=False) as tmp:
        tmp.write(data)
        p = tmp.name
    try:
        wb = xlrd.open_workbook(p)
        sh = wb.sheet_by_name(sheet)
        headers = [str(v).strip() for v in sh.row_values(5)]
        rows = []
        for r in range(6, sh.nrows):
            vals = sh.row_values(r)
            row = {}
            for i, h in enumerate(headers):
                row[h] = vals[i] if i < len(vals) else None
            rows.append(row)
        return rows
    finally:
        Path(p).unlink()


# ---------------------------------------------------------------------------
# 2013
# ---------------------------------------------------------------------------


def process_2013() -> list[dict]:
    rows = []
    zip_inc = RAW_DIR / "2013_income.zip"
    download(SOURCES[2013]["income"][0], zip_inc)
    data = read_from_zip(zip_inc, SOURCES[2013]["income"][1])
    inc_rows = read_xls_codes(data)

    zip_pov = RAW_DIR / "2013_poverty.zip"
    download(SOURCES[2013]["poverty"][0], zip_pov)
    data = read_from_zip(zip_pov, SOURCES[2013]["poverty"][1])
    pov_rows = read_xls_codes(data)

    pov_rate = {r["CODGEO"]: pf(r.get("TP6013")) for r in pov_rows}

    for r in inc_rows:
        c = r.get("CODGEO", "")
        if not c:
            continue
        rows.append(
            make_row(
                2013,
                "filosofi_1",
                c,
                nb_tax_households=r.get("NBMEN13"),
                nb_persons=r.get("NBPERS13"),
                median_income=r.get("Q213"),
                poverty_rate=pov_rate.get(c),
                decile1=r.get("D113"),
                decile9=r.get("D913"),
                gini=r.get("GI13"),
                s80_s20=r.get("S80S2013"),
                activity_income_share=r.get("PTSAC13"),
                salary_share=r.get("PTSAC13"),
                pension_share=r.get("PPEN13"),
                property_income_share=r.get("PPAT13"),
                social_benefits_share=r.get("PPSOC13"),
                family_benefits_share=r.get("PPFAM13"),
                minimum_social_share=r.get("PPMINI13"),
                housing_benefits_share=r.get("PPLOGT13"),
                tax_share=r.get("PIMPOT13"),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# 2014 — merged XLS
# ---------------------------------------------------------------------------


def process_2014() -> list[dict]:
    zip_path = RAW_DIR / "2014.zip"
    download(SOURCES[2014]["merged"][0], zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.filename.endswith(".xls"):
                data = zf.read(info)
                break

    with tempfile.NamedTemporaryFile(suffix=".xls", delete=False) as tmp:
        tmp.write(data)
        p = tmp.name
    try:
        wb = xlrd.open_workbook(p)
        sh = wb.sheet_by_name("COM")
        codes = [str(v).strip() for v in sh.row_values(5)]
        rows = []
        for r in range(6, sh.nrows):
            vals = sh.row_values(r)
            row = dict(zip(codes, vals, strict=False))
            c = row.get("CODGEO", "")
            if not c:
                continue
            rows.append(
                make_row(
                    2014,
                    "filosofi_1",
                    c,
                    nb_tax_households=row.get("NBMENFISC14"),
                    nb_persons=row.get("NBPERSMENFISC14"),
                    median_income=row.get("MED14"),
                    poverty_rate=row.get("TP6014"),
                    decile1=row.get("D114"),
                    decile9=row.get("D914"),
                    s80_s20=None,
                    activity_income_share=row.get("PRA14"),
                    salary_share=row.get("PTSAC14"),
                    pension_share=row.get("PPEN14"),
                    property_income_share=row.get("PPAT14"),
                    social_benefits_share=row.get("PPSOC14"),
                    family_benefits_share=row.get("PPFAM14"),
                    minimum_social_share=row.get("PPMINI14"),
                    housing_benefits_share=row.get("PPLOGT14"),
                    tax_share=row.get("PIMPOT14"),
                )
            )
        return rows
    finally:
        Path(p).unlink()


# ---------------------------------------------------------------------------
# 2015
# ---------------------------------------------------------------------------


def process_2015() -> list[dict]:
    rows = []
    zip_inc = RAW_DIR / "2015_income.zip"
    download(SOURCES[2015]["income"][0], zip_inc)
    data = read_from_zip(zip_inc, SOURCES[2015]["income"][1])
    inc_rows = read_xls_codes(data)

    zip_pov = RAW_DIR / "2015_poverty.zip"
    download(SOURCES[2015]["poverty"][0], zip_pov)
    data = read_from_zip(zip_pov, SOURCES[2015]["poverty"][1])
    pov_rows = read_xls_codes(data)

    pov_rate = {r["CODGEO"]: pf(r.get("TP6015")) for r in pov_rows}

    for r in inc_rows:
        c = r.get("CODGEO", "")
        if not c:
            continue
        rows.append(
            make_row(
                2015,
                "filosofi_1",
                c,
                nb_tax_households=r.get("NBMEN15"),
                nb_persons=r.get("NBPERS15"),
                median_income=r.get("Q215"),
                poverty_rate=pov_rate.get(c),
                decile1=r.get("D115"),
                decile9=r.get("D915"),
                gini=r.get("GI15"),
                s80_s20=r.get("S80S2015"),
                activity_income_share=r.get("PACT15"),
                salary_share=r.get("PTSA15"),
                unemployment_share=r.get("PCHO15"),
                pension_share=r.get("PPEN15"),
                property_income_share=r.get("PPAT15"),
                social_benefits_share=r.get("PPSOC15"),
                family_benefits_share=r.get("PPFAM15"),
                minimum_social_share=r.get("PPMINI15"),
                housing_benefits_share=r.get("PPLOGT15"),
                tax_share=r.get("PIMPOT15"),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# 2016
# ---------------------------------------------------------------------------


def process_2016() -> list[dict]:
    rows = []
    zip_inc = RAW_DIR / "2016_income.zip"
    download(SOURCES[2016]["income"][0], zip_inc)
    data = read_from_zip(zip_inc, SOURCES[2016]["income"][1])
    inc_rows = read_xls_codes(data)

    zip_pov = RAW_DIR / "2016_poverty.zip"
    download(SOURCES[2016]["poverty"][0], zip_pov)
    data = read_from_zip(zip_pov, SOURCES[2016]["poverty"][1])
    pov_rows = read_xls_codes(data)

    pov_rate = {r["CODGEO"]: pf(r.get("TP6016")) for r in pov_rows}

    for r in inc_rows:
        c = r.get("CODGEO", "")
        if not c:
            continue
        rows.append(
            make_row(
                2016,
                "filosofi_1",
                c,
                nb_tax_households=r.get("NBMEN16"),
                nb_persons=r.get("NBPERS16"),
                median_income=r.get("Q216"),
                poverty_rate=pov_rate.get(c),
                decile1=r.get("D116"),
                decile9=r.get("D916"),
                gini=r.get("GI16"),
                s80_s20=r.get("S80S2016"),
                activity_income_share=r.get("PACT16"),
                salary_share=r.get("PTSA16"),
                unemployment_share=r.get("PCHO16"),
                pension_share=r.get("PPEN16"),
                property_income_share=r.get("PPAT16"),
                social_benefits_share=r.get("PPSOC16"),
                family_benefits_share=r.get("PPFAM16"),
                minimum_social_share=r.get("PPMINI16"),
                housing_benefits_share=r.get("PPLOGT16"),
                tax_share=r.get("PIMPOT16"),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# 2017 — merged CSV
# ---------------------------------------------------------------------------


def process_2017() -> list[dict]:
    zip_path = RAW_DIR / "2017.zip"
    download(SOURCES[2017]["merged"][0], zip_path)
    data = read_from_zip(zip_path, SOURCES[2017]["merged"][1]).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(data), delimiter=";")
    rows = []
    for r in reader:
        c = r.get("CODGEO", "").strip()
        if not c:
            continue
        rows.append(
            make_row(
                2017,
                "filosofi_1",
                c,
                nb_tax_households=r.get("NBMENFISC17"),
                nb_persons=r.get("NBPERSMENFISC17"),
                median_income=r.get("MED17"),
                poverty_rate=r.get("TP6017"),
                decile1=r.get("D117"),
                decile9=r.get("D917"),
                gini=None,
                s80_s20=None,
                activity_income_share=r.get("PACT17"),
                salary_share=r.get("PTSA17"),
                unemployment_share=r.get("PCHO17"),
                pension_share=r.get("PPEN17"),
                property_income_share=r.get("PPAT17"),
                social_benefits_share=r.get("PPSOC17"),
                family_benefits_share=r.get("PPFAM17"),
                minimum_social_share=r.get("PPMINI17"),
                housing_benefits_share=r.get("PPLOGT17"),
                tax_share=r.get("PIMPOT17"),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# 2018–2019
# ---------------------------------------------------------------------------


def process_csv_inc_pov(
    year: int, url_inc: str, fname_inc: str, url_pov: str, fname_pov: str
) -> list[dict]:
    rows = []
    suf = str(year)[-2:]

    zip_inc = RAW_DIR / f"{year}_income.zip"
    download(url_inc, zip_inc)
    data = read_from_zip(zip_inc, fname_inc).decode("utf-8-sig")
    inc_rows = list(csv.DictReader(io.StringIO(data), delimiter=";"))

    zip_pov = RAW_DIR / f"{year}_poverty.zip"
    download(url_pov, zip_pov)
    data = read_from_zip(zip_pov, fname_pov).decode("utf-8-sig")
    pov_rows = list(csv.DictReader(io.StringIO(data), delimiter=";"))

    pov_rate = {r.get("CODGEO", ""): pf(r.get(f"TP60{suf}")) for r in pov_rows}

    for r in inc_rows:
        c = r.get("CODGEO", "").strip()
        if not c:
            continue
        rows.append(
            make_row(
                year,
                "filosofi_1",
                c,
                nb_tax_households=r.get(f"NBMEN{suf}"),
                nb_persons=r.get(f"NBPERS{suf}"),
                median_income=r.get(f"Q2{suf}"),
                poverty_rate=pov_rate.get(c),
                decile1=r.get(f"D1{suf}"),
                decile9=r.get(f"D9{suf}"),
                gini=r.get(f"GI{suf}"),
                s80_s20=r.get(f"S80S20{suf}"),
                activity_income_share=r.get(f"PACT{suf}"),
                salary_share=r.get(f"PTSA{suf}"),
                unemployment_share=r.get(f"PCHO{suf}"),
                pension_share=r.get(f"PPEN{suf}"),
                property_income_share=r.get(f"PPAT{suf}"),
                social_benefits_share=r.get(f"PPSOC{suf}"),
                family_benefits_share=r.get(f"PPFAM{suf}"),
                minimum_social_share=r.get(f"PPMINI{suf}"),
                housing_benefits_share=r.get(f"PPLOGT{suf}"),
                tax_share=r.get(f"PIMPOT{suf}"),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# 2020–2021 (limited: only PACT for composition)
# ---------------------------------------------------------------------------


def process_csv_limited(
    year: int, url_inc: str, fname_inc: str, url_pov: str, fname_pov: str
) -> list[dict]:
    rows = []
    suf = str(year)[-2:]

    zip_inc = RAW_DIR / f"{year}_income.zip"
    download(url_inc, zip_inc)
    data = read_from_zip(zip_inc, fname_inc).decode("utf-8-sig")
    inc_rows = list(csv.DictReader(io.StringIO(data), delimiter=";"))

    zip_pov = RAW_DIR / f"{year}_poverty.zip"
    download(url_pov, zip_pov)
    data = read_from_zip(zip_pov, fname_pov).decode("utf-8-sig")
    pov_rows = list(csv.DictReader(io.StringIO(data), delimiter=";"))

    pov_rate = {r.get("CODGEO", ""): pf(r.get(f"TP60{suf}")) for r in pov_rows}

    for r in inc_rows:
        c = r.get("CODGEO", "").strip()
        if not c:
            continue
        rows.append(
            make_row(
                year,
                "filosofi_1",
                c,
                nb_tax_households=r.get(f"NBMEN{suf}"),
                nb_persons=r.get(f"NBPERS{suf}"),
                median_income=r.get(f"Q2{suf}"),
                poverty_rate=pov_rate.get(c),
                decile1=r.get(f"D1{suf}"),
                decile9=r.get(f"D9{suf}"),
                gini=r.get(f"GI{suf}"),
                s80_s20=r.get(f"S80S20{suf}"),
                activity_income_share=r.get(f"PACT{suf}"),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# 2023 — Filosofi 2 long format
# ---------------------------------------------------------------------------

MEASURE_MAP_2023 = {
    "MED_SL": "median_income",
    "PR_MD60": "poverty_rate",
    "D1_SL": "decile1",
    "D9_SL": "decile9",
    "GI_SL": "gini",
    "S80S20_SL": "s80_s20",
    "S_EI_DI": "activity_income_share",
    "S_EI_DI_SAL": "salary_share",
    "S_EI_DI_UNE": "unemployment_share",
    "S_RET_PEN_DI": "pension_share",
    "S_INC_ASS_DI": "property_income_share",
    "S_SOC_BEN_DI": "social_benefits_share",
    "S_SOC_BEN_DI_FAM_BEN": "family_benefits_share",
    "S_SOC_BEN_DI_MIN_SOC": "minimum_social_share",
    "S_SOC_BEN_DI_HOU_BEN": "housing_benefits_share",
    "S_DIR_TAX_DI": "tax_share",
}


def process_2023() -> list[dict]:
    zip_path = RAW_DIR / "2023.zip"
    download(SOURCES[2023]["all"][0], zip_path)
    data = read_from_zip(zip_path, SOURCES[2023]["all"][1]).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(data), delimiter=";")

    comms: dict[str, dict] = {}
    for row in reader:
        geo_obj = row.get("GEO_OBJECT", "").strip().strip('"')
        if geo_obj != "COM":
            continue
        geo = row.get("GEO", "").strip().strip('"')
        measure = row.get("FILOSOFI_MEASURE", "").strip().strip('"')
        val = row.get("OBS_VALUE", "").strip()
        if geo not in comms:
            comms[geo] = {}
        comms[geo][measure] = val

    rows = []
    for geo, measures in comms.items():
        kw = {}
        for m_col, t_col in MEASURE_MAP_2023.items():
            kw[t_col] = measures.get(m_col)
        rows.append(make_row(2023, "filosofi_2", geo, **kw))
    return rows


# ---------------------------------------------------------------------------
# Source definitions
# ---------------------------------------------------------------------------

SOURCES: dict = {
    2013: {
        "income": (
            "https://www.insee.fr/fr/statistiques/fichier/2388413/indic-struct-distrib-revenu-2013-COMMUNES.zip",
            "FILO_DISP_COM.xls",
        ),
        "poverty": (
            "https://www.insee.fr/fr/statistiques/fichier/2388413/indic-struct-distrib-revenu-2013-COMMUNES.zip",
            "FILO_DISP_Pauvres_COM.xls",
        ),
    },
    2014: {
        "merged": (
            "https://www.insee.fr/fr/statistiques/fichier/3126432/filo-revenu-pauvrete-menage-2014.zip",
            "base-cc-filosofi-2014.xls",
        ),
    },
    2015: {
        "income": (
            "https://www.insee.fr/fr/statistiques/fichier/3560118/indic-struct-distrib-revenu-2015-COMMUNES.zip",
            "FILO_DISP_COM.xls",
        ),
        "poverty": (
            "https://www.insee.fr/fr/statistiques/fichier/3560118/indic-struct-distrib-revenu-2015-COMMUNES.zip",
            "FILO_DISP_Pauvres_COM.xls",
        ),
    },
    2016: {
        "income": (
            "https://www.insee.fr/fr/statistiques/fichier/4190006/indic-struct-distrib-revenu-2016-COMMUNES.zip",
            "FILO2016_DISP_COM.xls",
        ),
        "poverty": (
            "https://www.insee.fr/fr/statistiques/fichier/4190006/indic-struct-distrib-revenu-2016-COMMUNES.zip",
            "FILO2016_DISP_Pauvres_COM.xls",
        ),
    },
    2017: {
        "merged": (
            "https://www.insee.fr/fr/statistiques/fichier/4507225/base-filosofi-2017_CSV.zip",
            "cc_filosofi_2017_COM.CSV",
        ),
    },
    2018: {
        "income": (
            "https://www.insee.fr/fr/statistiques/fichier/5009218/indic-struct-distrib-revenu-2018-COMMUNES_csv.zip",
            "FILO2018_DISP_COM.csv",
        ),
        "poverty": (
            "https://www.insee.fr/fr/statistiques/fichier/5009218/indic-struct-distrib-revenu-2018-COMMUNES_csv.zip",
            "FILO2018_DISP_Pauvres_COM.csv",
        ),
    },
    2019: {
        "income": (
            "https://www.insee.fr/fr/statistiques/fichier/6036907/indic-struct-distrib-revenu-2019-COMMUNES_csv.zip",
            "FILO2019_DISP_COM.csv",
        ),
        "poverty": (
            "https://www.insee.fr/fr/statistiques/fichier/6036907/indic-struct-distrib-revenu-2019-COMMUNES_csv.zip",
            "FILO2019_DISP_Pauvres_COM.csv",
        ),
    },
    2020: {
        "income": (
            "https://www.insee.fr/fr/statistiques/fichier/6692220/indic-struct-distrib-revenu-2020-COMMUNES_csv.zip",
            "FILO2020_DISP_COM.csv",
        ),
        "poverty": (
            "https://www.insee.fr/fr/statistiques/fichier/6692220/indic-struct-distrib-revenu-2020-COMMUNES_csv.zip",
            "FILO2020_DISP_PAUVRES_COM.csv",
        ),
    },
    2021: {
        "income": (
            "https://www.insee.fr/fr/statistiques/fichier/7756855/indic-struct-distrib-revenu-2021-COMMUNES_csv.zip",
            "FILO2021_DISP_COM.csv",
        ),
        "poverty": (
            "https://www.insee.fr/fr/statistiques/fichier/7756855/indic-struct-distrib-revenu-2021-COMMUNES_csv.zip",
            "FILO2021_DISP_PAUVRES_COM.csv",
        ),
    },
    2023: {
        "all": (
            "https://www.insee.fr/fr/statistiques/fichier/8984752/FILOSOFI_CC_csv.zip",
            "DS_FILOSOFI_CC_2023_data.csv",
        ),
    },
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def upload_to_minio(csv_path: Path) -> None:
    """Upload the harmonized CSV to MinIO staging (bronze layer)."""
    from flows_staging.shared.download import (  # noqa: PLC0415
        _add_timestamp_to_filename,
    )
    from flows_staging.shared.minio import _upload_file_to_staging  # noqa: PLC0415
    from flows_staging.shared.minio import get_minio_client  # noqa: PLC0415

    minio = get_minio_client()
    bucket = "staging-current"
    folder = "income"
    base_name = "filosofi_income"

    timestamped = _add_timestamp_to_filename(base_name, ".csv")
    file_location = f"{folder}/{timestamped}"

    _upload_file_to_staging(minio, csv_path, bucket, file_location)

    size_mb = round(csv_path.stat().st_size / (1024 * 1024), 2)
    print(f"✅ Uploaded to s3://{bucket}/{file_location} ({size_mb} MB)")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []

    handlers = {
        2013: process_2013,
        2014: process_2014,
        2015: process_2015,
        2016: process_2016,
        2017: process_2017,
        2018: lambda: process_csv_inc_pov(
            2018,
            SOURCES[2018]["income"][0],
            SOURCES[2018]["income"][1],
            SOURCES[2018]["poverty"][0],
            SOURCES[2018]["poverty"][1],
        ),
        2019: lambda: process_csv_inc_pov(
            2019,
            SOURCES[2019]["income"][0],
            SOURCES[2019]["income"][1],
            SOURCES[2019]["poverty"][0],
            SOURCES[2019]["poverty"][1],
        ),
        2020: lambda: process_csv_limited(
            2020,
            SOURCES[2020]["income"][0],
            SOURCES[2020]["income"][1],
            SOURCES[2020]["poverty"][0],
            SOURCES[2020]["poverty"][1],
        ),
        2021: lambda: process_csv_limited(
            2021,
            SOURCES[2021]["income"][0],
            SOURCES[2021]["income"][1],
            SOURCES[2021]["poverty"][0],
            SOURCES[2021]["poverty"][1],
        ),
        2023: process_2023,
    }

    for year in sorted(handlers):
        print(f"\n=== {year} ===")
        rows = handlers[year]()
        all_rows.extend(rows)
        print(f"  → {len(rows)} communes")

    # Dedup
    seen: set[tuple[str, int]] = set()
    deduped = []
    for row in all_rows:
        key = (row.get("id", ""), row.get("year", 0))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    csv_path = Path("/tmp/filosofi_income_harmonized.csv")
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(deduped)

    print(f"\n✅ Written to {csv_path}")
    print(f"   Total rows: {len(deduped)}")
    by_yr = {}
    for r in deduped:
        by_yr[r["year"]] = by_yr.get(r["year"], 0) + 1
    for y in sorted(by_yr):
        print(f"   {y}: {by_yr[y]}")

    # Upload
    upload_to_minio(csv_path)
    csv_path.unlink()
    print("Done.")


if __name__ == "__main__":
    main()
