"""Download BD ALTI® 25m COG to local cache with resume support."""

import time
from pathlib import Path

import httpx
from tqdm import tqdm


URL = "https://data.geopf.fr/telechargement/download/archive_BDALTI_COG/archive/MNT_FRANCE-BDALTI_25M_L93_lzw.COG.TIF"
DEST = Path("/home/eric/data/bdalti") / "MNT_FRANCE-BDALTI_25M_L93_lzw.COG.TIF"

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
EXPECTED = 1_397_251_673


def download() -> None:
    DEST.parent.mkdir(parents=True, exist_ok=True)

    if DEST.exists() and DEST.stat().st_size >= EXPECTED:
        print(f"Already downloaded: {DEST} ({DEST.stat().st_size / 1024**3:.1f} GB)")
        return

    downloaded = DEST.stat().st_size if DEST.exists() else 0
    headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}

    for attempt in range(5):
        try:
            with httpx.stream(
                "GET", URL, headers=headers, follow_redirects=True, timeout=600
            ) as resp:
                if downloaded and resp.status_code == 206:
                    mode = "ab"
                elif not downloaded and resp.status_code == 200:
                    mode = "wb"
                elif downloaded and resp.status_code == 200:
                    # Server doesn't support resume — start over
                    downloaded = 0
                    headers = {}
                    mode = "wb"
                else:
                    resp.raise_for_status()
                    continue

                total = EXPECTED
                desc = f"{'Resuming' if downloaded else 'Downloading'} {total / 1024**3:.1f} GB COG"

                with (
                    DEST.open(mode) as f,
                    tqdm(
                        total=total,
                        initial=downloaded,
                        unit="B",
                        unit_scale=True,
                        desc=desc,
                        miniters=1,
                    ) as pbar,
                ):
                    for chunk in resp.iter_bytes(CHUNK_SIZE):
                        f.write(chunk)
                        pbar.update(len(chunk))

            final_size = DEST.stat().st_size
            if final_size >= EXPECTED:
                print(f"Done: {DEST} ({final_size / 1024**3:.1f} GB)")
                return
            # Partial — resume on next attempt
            downloaded = final_size
            headers = {"Range": f"bytes={downloaded}-"}
            print(f"Partial ({final_size}/{EXPECTED}), resuming…")
            time.sleep(3)

        except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError) as e:
            downloaded = DEST.stat().st_size if DEST.exists() else 0
            headers = {"Range": f"bytes={downloaded}-"}
            print(f"Connection issue (attempt {attempt + 1}/5): {e}")
            if attempt < 4:
                time.sleep(5 * (attempt + 1))

    raise RuntimeError(
        f"Failed to download after 5 attempts ({DEST.stat().st_size}/{EXPECTED} bytes)"
    )


if __name__ == "__main__":
    download()
