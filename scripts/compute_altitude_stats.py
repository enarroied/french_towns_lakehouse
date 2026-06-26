"""Compute altitude stats (min/max/mean) per commune from BD ALTI® 25m COG.

Usage:
    uv run python scripts/compute_altitude_stats.py --test     # Test on 5 communes
    uv run python scripts/compute_altitude_stats.py --full     # Full 35,000 communes
"""

import argparse
import csv
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

import duckdb
import numpy as np
import rasterio
from pyproj import Transformer
from rasterio.features import geometry_mask
from rasterio.windows import from_bounds
from shapely import wkb
from shapely.ops import transform


COG_PATH = "/home/eric/data/bdalti/MNT_FRANCE-BDALTI_25M_L93_lzw.COG.TIF"
OUTPUT = Path("altitude_stats.csv")

BATCH_SIZE = 10_000

crs_transformer = Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True)


def reproject_to_2154(geom_wkb_hex: str):
    """Reproject geometry from 4326 to 2154 using pyproj."""
    geom = wkb.loads(bytes.fromhex(geom_wkb_hex))
    return transform(crs_transformer.transform, geom)


def get_communes(limit: int | None = 50000, test: bool = False) -> list[tuple]:
    """Fetch communes with geometry from dim_geography.parquet (EPSG:4326)."""
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    query = """
        SELECT
            commune_id,
            ST_AsWKB(geometry) AS geom_wkb
        FROM read_parquet('s3://validated/dim_geography.parquet')
        WHERE geometry IS NOT NULL
    """
    if test:
        query += """
            AND commune_id IN ('75056', '13055', '69381', '31069', '06004')
        """

    results = conn.execute(query).fetchall()

    communes = []
    for row in results:
        commune_id, geom_wkb = row
        if geom_wkb is None:
            continue
        communes.append((commune_id, geom_wkb.hex()))

    if limit and len(communes) > limit:
        communes = communes[:limit]

    return communes


def compute_one(args: tuple) -> tuple[str, int | None, int | None, float | None]:
    """Compute altitude stats for a single commune.

    Returns (commune_id, min_alt, max_alt, mean_alt).
    """
    commune_id, geom_wkb_hex = args

    try:
        geom_2154 = reproject_to_2154(geom_wkb_hex)

        with rasterio.open(COG_PATH) as src:
            minx, miny, maxx, maxy = geom_2154.bounds

            if maxx <= minx or maxy <= miny:
                return (commune_id, None, None, None)

            # Check bounds overlap with COG extent
            cog_left, cog_bottom, cog_right, cog_top = src.bounds
            if (
                maxx < cog_left
                or minx > cog_right
                or maxy < cog_bottom
                or miny > cog_top
            ):
                return (commune_id, None, None, None)

            # Read window covering the commune
            window = from_bounds(minx, miny, maxx, maxy, src.transform)
            data = src.read(1, window=window, masked=False).astype(np.float64)
            w_transform = src.window_transform(window)

            # Mask for pixels strictly inside the polygon
            mask = geometry_mask(
                [geom_2154],
                transform=w_transform,
                out_shape=data.shape,
                invert=True,
            )

            valid = data[mask]
            valid = valid[valid > 0]

            if len(valid) == 0:
                return (commune_id, None, None, None)

            min_alt = int(np.min(valid))
            max_alt = int(np.max(valid))
            mean_alt = round(float(np.mean(valid)), 1)

            return (commune_id, min_alt, max_alt, mean_alt)

    except Exception as e:
        print(f"  ERROR {commune_id}: {e}")
        return (commune_id, None, None, None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Test on 5 communes")
    parser.add_argument("--full", action="store_true", help="Full 35,000 communes")
    args = parser.parse_args()

    if not args.test and not args.full:
        parser.print_help()
        return

    test_mode = args.test
    limit = 5 if test_mode else 100000

    print(f"Fetching communes ({'test: 5' if test_mode else 'full ~35000'})…")
    communes = get_communes(limit=limit, test=test_mode)
    print(f"Got {len(communes)} communes")

    workers = 4 if test_mode else 8
    print(f"Computing altitude stats with {workers} workers…")

    start = time.time()
    results = []

    for i in range(0, len(communes), BATCH_SIZE):
        batch = communes[i : i + BATCH_SIZE]
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(compute_one, c): c[0] for c in batch}
            for f in as_completed(futures):
                cid = futures[f]
                try:
                    result = f.result()
                    results.append(result)
                except Exception as e:
                    print(f"  FAIL {cid}: {e}")
                    results.append((cid, None, None, None))

        elapsed = time.time() - start
        print(f"  Progress: {len(results)}/{len(communes)} ({elapsed:.0f}s)")

    elapsed = time.time() - start
    print(f"Done in {elapsed:.0f}s for {len(results)} communes")

    # Write CSV
    with OUTPUT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["commune_id", "altitude_min", "altitude_max", "altitude_moyenne"])
        w.writerows(results)

    print(f"Written to {OUTPUT}")

    # Summary
    with_vals = [r for r in results if r[1] is not None]
    print(f"  With altitude data: {len(with_vals)}/{len(results)}")
    if with_vals:
        means = [r[3] for r in with_vals if r[3] is not None]
        print(f"  Mean altitude (across communes): {np.mean(means):.0f}m")
        print(f"  Min altitude overall: {min(r[1] for r in with_vals)}m")
        print(f"  Max altitude overall: {max(r[2] for r in with_vals)}m")


if __name__ == "__main__":
    main()
