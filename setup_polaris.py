"""One-time Polaris setup: create the french_towns catalog and lakehouse namespace.

Phase 1 — Create the catalog via Polaris Management REST API.
Phase 2 — Attach via DuckDB and create the lakehouse namespace.

Idempotent — safe to re-run.
"""

import os
import sys

import httpx
from dotenv import find_dotenv
from dotenv import load_dotenv
from flows_integration.shared.connection import get_duckdb_connection


load_dotenv(find_dotenv())

POLARIS_URL = "http://localhost:8181"
CLIENT_ID = os.environ["POLARIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["POLARIS_CLIENT_SECRET"]
REALM = os.environ.get("POLARIS_REALM", "POLARIS")


def _polaris_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Polaris-Realm": REALM,
    }


def _check(resp: httpx.Response, label: str) -> None:
    if resp.status_code not in (200, 201, 204):
        print(f"❌ {label} failed ({resp.status_code})")
        print(f"   Response body: {resp.text}")
        sys.exit(1)
    print(f"✓ {label}")


def _get_token() -> str:
    with httpx.Client() as client:
        resp = client.post(
            f"{POLARIS_URL}/api/catalog/v1/oauth/tokens",
            auth=(CLIENT_ID, CLIENT_SECRET),
            headers={"Polaris-Realm": REALM},
            data={
                "grant_type": "client_credentials",
                "scope": "PRINCIPAL_ROLE:ALL",
            },
        )
    _check(resp, "OAuth2 token")
    token: str = resp.json()["access_token"]
    return token


def _catalog_exists(token: str, name: str) -> bool:
    with httpx.Client() as client:
        resp = client.get(
            f"{POLARIS_URL}/api/management/v1/catalogs/{name}",
            headers=_polaris_headers(token),
        )
    return resp.status_code == 200


def _create_catalog(token: str) -> None:
    name = "french_towns"

    if _catalog_exists(token, name):
        print(f"✓ Catalog '{name}' already exists")
        return

    body = {
        "catalog": {
            "name": name,
            "type": "INTERNAL",
            "properties": {
                "default-base-location": "s3://lakehouse/",
                "s3.endpoint": "http://minio:9000",
                "s3.region": "us-east-1",
                "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
                "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": ["s3://lakehouse/"],
            },
        },
    }

    with httpx.Client() as client:
        resp = client.post(
            f"{POLARIS_URL}/api/management/v1/catalogs",
            headers=_polaris_headers(token),
            json=body,
        )
    _check(resp, "Catalog creation")
    print(f"   Catalog '{name}' created")


def _setup_lakehouse() -> None:
    conn = get_duckdb_connection()
    conn.execute("CREATE SCHEMA IF NOT EXISTS polaris.lakehouse;")
    conn.close()

    print("✓ Attached 'french_towns' via DuckDB")
    print("✓ Namespace 'lakehouse' ready")


def main() -> None:
    print("=== Polaris Setup ===\n")

    print("── Phase 1: Catalog via Management API ──")
    token = _get_token()
    _create_catalog(token)

    print()
    print("── Phase 2: Lakehouse namespace via DuckDB ──")
    _setup_lakehouse()

    print()
    print("=== Setup complete ===")
    print()
    print("To query your lakehouse:")
    print()
    print("    ATTACH 'french_towns' AS polaris (")
    print("        TYPE iceberg,")
    print("        ENDPOINT 'http://localhost:8181/api/catalog',")
    print("        SECRET 'polaris_secret'")
    print("    );")
    print("    SELECT * FROM polaris.lakehouse.dim_communes_france;")


if __name__ == "__main__":
    main()
