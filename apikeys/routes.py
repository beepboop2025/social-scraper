"""FastAPI routes for API key management — integrated into econscraper API."""

import logging
import os
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/keys", tags=["api_keys"])


def verify_admin_key(x_admin_key: str = Header(...)):
    """Verify the admin API key from the X-Admin-Key header."""
    expected = os.getenv("ADMIN_API_KEY", "")
    if not expected or x_admin_key != expected:
        raise HTTPException(status_code=403, detail="Invalid or missing admin key")
    return x_admin_key


class StoreKeyRequest(BaseModel):
    key: str


@router.get("/status")
async def key_status():
    """Show which API keys are configured vs missing."""
    from apikeys.injector import KeyInjector

    injector = KeyInjector()
    return {
        "configured": injector.get_configured_keys(),
        "missing": injector.get_missing_keys(),
    }


@router.get("/catalog")
async def key_catalog(
    category: Optional[str] = None,
    priority: Optional[str] = None,
):
    """Browse the full API catalog."""
    from apikeys.catalog import CATALOG, CATEGORIES, get_by_category, get_by_priority

    if category:
        apis = get_by_category(category)
    elif priority:
        apis = get_by_priority(priority)
    else:
        apis = CATALOG

    # Sanitize — don't expose test endpoints in API
    sanitized = {}
    for api_id, info in apis.items():
        sanitized[api_id] = {
            "name": info["name"],
            "provider": info.get("provider", ""),
            "signup_url": info["signup_url"],
            "free_tier": info["free_tier"],
            "signup_method": info.get("signup_method", ""),
            "category": info.get("category", ""),
            "priority": info.get("priority", ""),
            "env_var": info.get("env_var", ""),
            "used_by": info.get("used_by", []),
        }

    return {"count": len(sanitized), "categories": CATEGORIES, "apis": sanitized}


@router.get("/catalog/{api_id}")
async def api_details(api_id: str):
    """Get detailed info and signup instructions for a specific API."""
    from apikeys.provisioner import KeyProvisioner

    prov = KeyProvisioner()
    info = prov.get_instructions(api_id)
    if not info:
        return {"error": f"Unknown API: {api_id}"}
    return info


@router.get("/plan")
async def setup_plan(priority: Optional[str] = None):
    """Get prioritized setup plan."""
    from apikeys.provisioner import KeyProvisioner

    prov = KeyProvisioner()
    plan = prov.get_setup_plan(priority=priority)
    estimate = prov.estimate_setup_time()
    return {"estimate": estimate, "plan": plan}


@router.get("/quickstart")
async def quickstart():
    """Get APIs that can be set up in under 2 minutes."""
    from apikeys.provisioner import KeyProvisioner

    prov = KeyProvisioner()
    return {
        "instant_apis": prov.get_quick_start_apis(),
        "no_key_needed": prov.get_no_key_apis(),
    }


@router.post("/store/{api_id}")
async def store_key(api_id: str, body: StoreKeyRequest, _admin=Depends(verify_admin_key)):
    """Store an API key in the encrypted vault."""
    from apikeys.catalog import CATALOG
    from apikeys.vault import KeyVault

    if api_id not in CATALOG:
        return {"error": f"Unknown API: {api_id}", "available": sorted(CATALOG.keys())}

    info = CATALOG[api_id]
    vault = KeyVault()
    vault.store(api_id, body.key, env_var=info.get("env_var", ""))

    return {
        "status": "stored",
        "api_id": api_id,
        "api_name": info["name"],
        "env_var": info.get("env_var", ""),
    }


@router.post("/validate/{api_id}")
async def validate_key(api_id: str, _admin=Depends(verify_admin_key)):
    """Validate a stored key by making a test API call."""
    from apikeys.validator import KeyValidator
    from apikeys.vault import KeyVault

    vault = KeyVault()
    key = vault.get(api_id)
    if not key:
        return {"error": f"No key stored for {api_id}"}

    validator = KeyValidator()
    result = validator.validate(api_id, key)
    vault.update_validation(api_id, result["is_valid"])

    return result


@router.post("/validate-all")
async def validate_all_keys(_admin=Depends(verify_admin_key)):
    """Validate all stored keys."""
    from apikeys.validator import KeyValidator
    from apikeys.vault import KeyVault

    vault = KeyVault()
    validator = KeyValidator()
    keys = vault.list_keys()

    results = []
    for api_id in keys:
        key = vault.get(api_id)
        result = validator.validate(api_id, key)
        vault.update_validation(api_id, result["is_valid"])
        results.append(result)

    valid = sum(1 for r in results if r["is_valid"] is True)
    invalid = sum(1 for r in results if r["is_valid"] is False)

    return {
        "total": len(results),
        "valid": valid,
        "invalid": invalid,
        "results": results,
    }


@router.post("/inject")
async def inject_keys(_admin=Depends(verify_admin_key)):
    """Inject all vault keys into .env file and runtime."""
    from apikeys.injector import KeyInjector

    injector = KeyInjector()
    result = injector.sync_from_vault()
    return result


@router.get("/vault")
async def list_vault(_admin=Depends(verify_admin_key)):
    """List all keys in the vault (masked)."""
    from apikeys.vault import KeyVault

    vault = KeyVault()
    return {"keys": vault.list_keys()}


@router.delete("/vault/{api_id}")
async def remove_key(api_id: str, _admin=Depends(verify_admin_key)):
    """Remove a key from the vault."""
    from apikeys.vault import KeyVault

    vault = KeyVault()
    removed = vault.remove(api_id)
    return {"removed": removed, "api_id": api_id}
