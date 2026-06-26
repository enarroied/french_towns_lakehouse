"""Generate island_communes.csv and upload to S3 staging.

Auto-classifies is_insular (whole-department islands) and
is_island_commune (specific mainland island communes from Wikipedia).
"""

import os
from datetime import datetime
from pathlib import Path

import boto3
import duckdb
import pandas as pd
from botocore.config import Config
from dotenv import find_dotenv
from dotenv import load_dotenv


load_dotenv(find_dotenv())

# ── Config ────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
STAGING_BUCKET = "staging-current"
TARGET_FOLDER = "geography"
BASE_NAME = "island_communes"

# Entire-department islands → all communes get is_insular = True
INSULAR_DEPARTMENTS: set[str] = {
    "2A",
    "2B",  # Corsica
    "971",  # Guadeloupe
    "972",  # Martinique
    "974",  # La Réunion
    "975",  # Saint-Pierre-et-Miquelon
    "976",  # Mayotte
    "977",  # Saint-Barthélemy
    "978",  # Saint-Martin
    "981",  # Île des Faisans
    "984",  # TAAF
    "986",  # Wallis-et-Futuna
    "987",  # Polynésie française
    "988",  # Nouvelle-Calédonie
    "989",  # Clipperton
}

# Specific mainland island communes → is_island_commune = True
# (commune_id → name, for documentation)
ISLAND_COMMUNES: dict[str, str] = {
    # Île de Ré (10)
    "17019": "Ars-en-Ré",
    "17051": "Le Bois-Plage-en-Ré",
    "17121": "La Couarde-sur-Mer",
    "17161": "La Flotte-en-Ré",
    "17207": "Loix",
    "17286": "Les Portes-en-Ré",
    "17297": "Rivedoux-Plage",
    "17318": "Saint-Clément-des-Baleines",
    "17369": "Saint-Martin-de-Ré",
    "17360": "Sainte-Marie-de-Ré",
    # Île d'Oléron (8)
    "17486": "La Brée-les-Bains",
    "17093": "Le Château-d'Oléron",
    "17140": "Dolus-d'Oléron",
    "17485": "Le Grand-Village-Plage",
    "17323": "Saint-Denis-d'Oléron",
    "17337": "Saint-Georges-d'Oléron",
    "17385": "Saint-Pierre-d'Oléron",
    "17411": "Saint-Trojan-les-Bains",
    # Belle-Île (4)
    "56009": "Bangor",
    "56114": "Locmaria",
    "56152": "Le Palais",
    "56241": "Sauzon",
    # Noirmoutier (4)
    "85011": "Barbâtre",
    "85083": "L'Épine",
    "85106": "La Guérinière",
    "85163": "Noirmoutier-en-l'Île",
    # Îles du Ponant (12)
    "17004": "Île-d'Aix",
    "56088": "Île-d'Arz",
    "29082": "Île-de-Batz",
    "22016": "Île-de-Bréhat",
    "56069": "Groix",
    "56085": "Hœdic",
    "56086": "Île-d'Houat",
    "56087": "Île-aux-Moines",
    "29084": "Île-Molène",
    "29155": "Ouessant",
    "29083": "Île-de-Sein",
    "85113": "Île-d'Yeu",
}


def main() -> None:
    print("Connecting to lakehouse…")
    conn = duckdb.connect()
    conn.execute("LOAD iceberg;")
    conn.execute(f"""
        CREATE SECRET polaris_secret (
            TYPE iceberg,
            CLIENT_ID '{os.environ["POLARIS_CLIENT_ID"]}',
            CLIENT_SECRET '{os.environ["POLARIS_CLIENT_SECRET"]}',
            ENDPOINT 'http://localhost:8181/api/catalog'
        )
    """)
    conn.execute("""
        ATTACH 'french_towns' AS polaris (
            TYPE iceberg,
            ENDPOINT 'http://localhost:8181/api/catalog',
            SECRET 'polaris_secret'
        )
    """)

    print("Fetching all communes…")
    df: pd.DataFrame = conn.execute("""
        SELECT id, name, department_code
        FROM polaris.lakehouse.dim_communes
        ORDER BY id
    """).fetchdf()

    print(f"  Loaded {len(df)} communes")

    # Apply is_insular
    df["is_insular"] = df["department_code"].isin(INSULAR_DEPARTMENTS)

    # Apply is_island_commune
    df["is_island_commune"] = df["id"].isin(set(ISLAND_COMMUNES.keys()))

    # Validate: all hardcoded commune IDs exist
    missing = [cid for cid in ISLAND_COMMUNES if cid not in set(df["id"])]
    if missing:
        print(
            f"  ⚠ WARNING: {len(missing)} commune IDs not found in lakehouse: {missing}"
        )
    else:
        print(f"  ✅ All {len(ISLAND_COMMUNES)} island communes found in lakehouse")

    # Summary
    n_insular = df["is_insular"].sum()
    n_island = df["is_island_commune"].sum()
    n_both = ((df["is_insular"]) & (df["is_island_commune"])).sum()
    print(f"  is_insular: {n_insular} communes")
    print(f"  is_island_commune: {n_island} communes")
    print(f"  (both): {n_both} communes")

    # Check: these should be separate — no overlap
    if n_both > 0:
        both_ids = df[df["is_insular"] & df["is_island_commune"]]["id"].tolist()
        print(f"  ⚠ {n_both} communes have both flags: {both_ids}")
    else:
        print("  ✅ No overlap between is_insular and is_island_commune")

    # Write CSV
    csv_path = Path(f"/tmp/{BASE_NAME}.csv")
    df_out = df[["id", "is_insular", "is_island_commune"]]
    df_out.to_csv(csv_path, index=False)
    print(f"  Written → {csv_path} ({csv_path.stat().st_size / 1024:.1f} KB)")

    # Upload to MinIO
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{TARGET_FOLDER}/{BASE_NAME}_{timestamp}.csv"

    print(f"Uploading to s3://{STAGING_BUCKET}/{s3_key} …")
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        use_ssl=False,
    )

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=STAGING_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=STAGING_BUCKET)

    s3.upload_file(str(csv_path), STAGING_BUCKET, s3_key)
    print("  ✅ Uploaded successfully")

    csv_path.unlink()
    print("Done.")


if __name__ == "__main__":
    main()
