"""One-time Polaris setup: create the french_towns catalog and lakehouse namespace.

Phase 1 — Create the catalog via Polaris Management REST API.
Phase 2 — Attach via DuckDB and create the lakehouse namespace.
Phase 3 — Grant table creation privileges to root principal.

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
CATALOG = "french_towns"
PRINCIPAL = "root"
PRINCIPAL_ROLE = "lakehouse_admin"
CATALOG_ROLE = "content_manager"


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


def _get_catalog(token: str) -> dict | None:
    with httpx.Client() as client:
        resp = client.get(
            f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG}",
            headers=_polaris_headers(token),
        )
    if resp.status_code != 200:
        return None
    return resp.json()


def _create_catalog(token: str) -> None:
    if _catalog_exists(token, CATALOG):
        print(f"✓ Catalog '{CATALOG}' already exists")
        return

    body = {
        "catalog": {
            "name": CATALOG,
            "type": "INTERNAL",
            "properties": {
                "default-base-location": "s3://lakehouse/",
                "s3.endpoint": "http://127.0.0.1:19000",
                "s3.region": "us-east-1",
                "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
                "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": ["s3://lakehouse/"],
                "region": "us-east-1",
                "endpoint": "http://127.0.0.1:19000",
                "endpointInternal": "http://minio:9000",
                "pathStyleAccess": True,
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
    print(f"   Catalog '{CATALOG}' created")


def _ensure_storage_config(token: str) -> None:
    catalog = _get_catalog(token)
    if catalog is None:
        return

    sc = catalog.get("storageConfigInfo", {})
    props = catalog.get("properties", {})
    needs_update = False

    for key, expected in (
        ("region", "us-east-1"),
        ("endpoint", "http://127.0.0.1:19000"),
        ("endpointInternal", "http://minio:9000"),
        ("pathStyleAccess", True),
    ):
        if sc.get(key) != expected:
            needs_update = True
            break

    expected_props = {
        "default-base-location": props.get("default-base-location"),
        "s3.endpoint": "http://127.0.0.1:19000",
        "s3.region": "us-east-1",
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
    }
    for k, v in expected_props.items():
        if props.get(k) != v:
            needs_update = True
            break

    if not needs_update:
        print("✓ Storage config already up-to-date")
        return

    body: dict = {
        "currentEntityVersion": catalog["entityVersion"],
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": ["s3://lakehouse/"],
            "region": "us-east-1",
            "endpoint": "http://127.0.0.1:19000",
            "endpointInternal": "http://minio:9000",
            "pathStyleAccess": True,
        },
    }
    updated_props = dict(props)
    updated_props["s3.endpoint"] = "http://127.0.0.1:19000"
    updated_props.setdefault("s3.region", "us-east-1")
    updated_props.setdefault("s3.access-key-id", os.environ["AWS_ACCESS_KEY_ID"])
    updated_props.setdefault(
        "s3.secret-access-key", os.environ["AWS_SECRET_ACCESS_KEY"]
    )
    if updated_props != props:
        body["properties"] = updated_props

    with httpx.Client() as client:
        resp = client.put(
            f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG}",
            headers=_polaris_headers(token),
            json=body,
        )
    _check(resp, "Storage config update")


def _setup_lakehouse() -> None:
    conn = get_duckdb_connection()
    conn.execute("CREATE SCHEMA IF NOT EXISTS polaris.lakehouse;")
    conn.close()

    print("✓ Attached 'french_towns' via DuckDB")
    print("✓ Namespace 'lakehouse' ready")


def _list_principal_roles(token: str) -> list[str]:
    with httpx.Client() as client:
        resp = client.get(
            f"{POLARIS_URL}/api/management/v1/principal-roles",
            headers=_polaris_headers(token),
        )
    if resp.status_code != 200:
        return []
    return [r["name"] for r in resp.json().get("roles", [])]


def _create_principal_role(token: str) -> None:
    existing = _list_principal_roles(token)
    if PRINCIPAL_ROLE in existing:
        print(f"✓ Principal role '{PRINCIPAL_ROLE}' already exists")
        return

    with httpx.Client() as client:
        resp = client.post(
            f"{POLARIS_URL}/api/management/v1/principal-roles",
            headers=_polaris_headers(token),
            json={"principalRole": {"name": PRINCIPAL_ROLE}},
        )
    _check(resp, f"Principal role '{PRINCIPAL_ROLE}' creation")


def _list_principal_roles_assigned(token: str) -> list[str]:
    with httpx.Client() as client:
        resp = client.get(
            f"{POLARIS_URL}/api/management/v1/principals/{PRINCIPAL}/principal-roles",
            headers=_polaris_headers(token),
        )
    if resp.status_code != 200:
        return []
    return [r["name"] for r in resp.json().get("roles", [])]


def _assign_principal_role(token: str) -> None:
    assigned = _list_principal_roles_assigned(token)
    if PRINCIPAL_ROLE in assigned:
        print(f"✓ Principal role '{PRINCIPAL_ROLE}' already assigned to '{PRINCIPAL}'")
        return

    with httpx.Client() as client:
        resp = client.put(
            f"{POLARIS_URL}/api/management/v1/principals/{PRINCIPAL}/principal-roles",
            headers=_polaris_headers(token),
            json={"principalRole": {"name": PRINCIPAL_ROLE}},
        )
    _check(resp, f"Assign '{PRINCIPAL_ROLE}' to '{PRINCIPAL}'")


def _list_catalog_roles(token: str) -> list[str]:
    with httpx.Client() as client:
        resp = client.get(
            f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG}/catalog-roles",
            headers=_polaris_headers(token),
        )
    if resp.status_code != 200:
        return []
    return [r["name"] for r in resp.json().get("roles", [])]


def _create_catalog_role(token: str) -> None:
    existing = _list_catalog_roles(token)
    if CATALOG_ROLE in existing:
        print(f"✓ Catalog role '{CATALOG_ROLE}' already exists")
        return

    with httpx.Client() as client:
        resp = client.post(
            f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG}/catalog-roles",
            headers=_polaris_headers(token),
            json={"catalogRole": {"name": CATALOG_ROLE}},
        )
    _check(resp, f"Catalog role '{CATALOG_ROLE}' creation")


def _grant_privileges(token: str) -> None:
    with httpx.Client() as client:
        resp = client.get(
            f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG}/catalog-roles/{CATALOG_ROLE}/grants",
            headers=_polaris_headers(token),
        )
    if resp.status_code == 200:
        privileges = [g["privilege"] for g in resp.json().get("grants", [])]
        if "CATALOG_MANAGE_CONTENT" in privileges:
            print("✓ Privilege 'CATALOG_MANAGE_CONTENT' already granted")
            return

    with httpx.Client() as client:
        resp = client.put(
            f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG}/catalog-roles/{CATALOG_ROLE}/grants",
            headers=_polaris_headers(token),
            json={
                "grant": {
                    "type": "catalog",
                    "privilege": "CATALOG_MANAGE_CONTENT",
                }
            },
        )
    _check(resp, "Grant 'CATALOG_MANAGE_CONTENT'")


def _list_catalog_role_assignments(token: str) -> list[str]:
    with httpx.Client() as client:
        resp = client.get(
            f"{POLARIS_URL}/api/management/v1/principal-roles/{PRINCIPAL_ROLE}/catalog-roles",
            headers=_polaris_headers(token),
        )
    if resp.status_code != 200:
        return []
    return [r["name"] for r in resp.json().get("roles", [])]


def _assign_catalog_role(token: str) -> None:
    assigned = _list_catalog_role_assignments(token)
    if CATALOG in assigned:
        print(
            f"✓ Catalog role '{CATALOG_ROLE}' already assigned to principal role '{PRINCIPAL_ROLE}'"
        )
        return

    with httpx.Client() as client:
        resp = client.put(
            f"{POLARIS_URL}/api/management/v1/principal-roles/{PRINCIPAL_ROLE}/catalog-roles/{CATALOG}",
            headers=_polaris_headers(token),
            json={"catalogRole": {"name": CATALOG_ROLE}},
        )
    _check(resp, f"Assign '{CATALOG_ROLE}' to '{PRINCIPAL_ROLE}'")


def _setup_rbac(token: str) -> None:
    print()
    print("── Phase 3: RBAC — grant table creation privileges ──")

    _create_principal_role(token)
    _assign_principal_role(token)
    _create_catalog_role(token)
    _grant_privileges(token)
    _assign_catalog_role(token)


def main() -> None:
    print("=== Polaris Setup ===\n")

    print("── Phase 1: Catalog via Management API ──")
    token = _get_token()
    _create_catalog(token)
    _ensure_storage_config(token)

    print()
    print("── Phase 2: Lakehouse namespace via DuckDB ──")
    _setup_lakehouse()

    _setup_rbac(token)

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
    print("    SELECT * FROM polaris.lakehouse.dim_communes;")
    print("    SELECT * FROM polaris.lakehouse.dim_geography;")


if __name__ == "__main__":
    main()
