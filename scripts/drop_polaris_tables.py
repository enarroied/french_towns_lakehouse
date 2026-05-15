"""
Drop all lakehouse Iceberg tables from the Polaris catalog via REST API.

Use this when MinIO data is wiped but Polaris still has stale table
registrations pointing to non-existent metadata files.

Idempotent — safe to run multiple times.
"""

import os

import httpx
from dotenv import find_dotenv
from dotenv import load_dotenv


load_dotenv(find_dotenv())

TABLES = [
    "dim_communes_france",
    "dim_zip_codes",
    "bridge_communes_zip_codes",
    "fact_population",
    "fact_salaries",
    "dim_neighbour_communes",
]

API_BASE = "http://localhost:8181/api/catalog/v1"
NAMESPACE = "lakehouse"
REALM = "POLARIS"
CLIENT_ID = os.environ["POLARIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["POLARIS_CLIENT_SECRET"]


def _get_token() -> str:
    resp = httpx.post(
        f"{API_BASE}/oauth/tokens",
        auth=(CLIENT_ID, CLIENT_SECRET),
        headers={"Polaris-Realm": REALM},
        data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"},
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def drop_table(token: str, table_name: str) -> None:
    url = f"{API_BASE}/namespaces/{NAMESPACE}/tables/{table_name}?purgeRequested=false"
    resp = httpx.delete(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": REALM,
        },
    )
    if resp.status_code == 204:
        print(f"  ✅ Dropped {table_name}")
    elif resp.status_code == 404:
        print(f"  ⏭️  Table {table_name} not found (already clean)")
    else:
        print(f"  ❌ Failed to drop {table_name}: {resp.status_code} {resp.text}")


def main() -> None:
    print("Authenticating with Polaris...")
    token = _get_token()
    print(f"Dropping {len(TABLES)} tables from {NAMESPACE} namespace...")
    for tbl in TABLES:
        drop_table(token, tbl)
    print("Done.")


if __name__ == "__main__":
    main()
