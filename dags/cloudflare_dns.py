import os
import re
from typing import Optional

import requests


def _sanitize_subdomain(label: str) -> str:
    label = (label or "").strip().lower().replace(" ", "-")
    # Keep it DNS-safe-ish: letters, digits, hyphen.
    label = re.sub(r"[^a-z0-9-]", "-", label)
    label = re.sub(r"-+", "-", label).strip("-")
    if not label:
        raise ValueError("Empty/invalid subdomain label")
    return label


def _cf_request(method: str, url: str, token: str, **kwargs):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    resp = requests.request(method, url, headers=headers, timeout=30, **kwargs)
    try:
        data = resp.json()
    except Exception:
        data = {"success": False, "errors": [{"message": resp.text}]}

    if not resp.ok or not data.get("success", False):
        raise RuntimeError(f"Cloudflare API error: {data}")
    return data


def upsert_a_record(
    *,
    zone_id: str,
    api_token: str,
    fqdn: str,
    ip: str,
    proxied: bool = False,
    ttl: int = 1,
) -> str:
    """Create or update a Cloudflare A record. Returns record id."""

    base = f"https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records"

    # Lookup existing A record
    q = {
        "type": "A",
        "name": fqdn,
        "per_page": 100,
    }
    existing = _cf_request("GET", base, api_token, params=q).get("result", [])

    payload = {
        "type": "A",
        "name": fqdn,
        "content": ip,
        "ttl": ttl,
        "proxied": proxied,
    }

    if existing:
        rec_id = existing[0]["id"]
        _cf_request("PUT", f"{base}/{rec_id}", api_token, json=payload)
        return rec_id

    created = _cf_request("POST", base, api_token, json=payload).get("result", {})
    return created.get("id")


def register_edge_records_for_clusters(
    *,
    zone_name: str,
    zone_id: str,
    api_token: str,
    edge_public_ip: str,
    clusters: list[dict],
    proxied: bool = False,
) -> None:
    """Registers per-cluster DNS records pointing to the edge VM.

    For each cluster_name, creates/updates:
      - <cluster>.zone_name
      - *.<cluster>.zone_name
    """
    if not edge_public_ip:
        raise ValueError("edge_public_ip is required")

    zone_name = (zone_name or "").strip().rstrip(".")
    if not zone_name:
        raise ValueError("zone_name is required")

    for c in clusters or []:
        raw_name = c.get("cluster_name") or c.get("clusterName")
        safe = _sanitize_subdomain(str(raw_name))

        apex = f"{safe}.{zone_name}"
        wildcard = f"*.{safe}.{zone_name}"

        upsert_a_record(zone_id=zone_id, api_token=api_token, fqdn=apex, ip=edge_public_ip, proxied=proxied)
        upsert_a_record(zone_id=zone_id, api_token=api_token, fqdn=wildcard, ip=edge_public_ip, proxied=proxied)


def register_edge_records_for_resource(
        *,
        zone_name: str,
        zone_id: str,
        api_token: str,
        edge_public_ip: str,
        resource_slug: str,
        proxied: bool = False,
) -> None:
        """Registers DNS records for a resource-scoped edge.

        Creates/updates:
            - <resource_slug>.zone_name
            - *.<resource_slug>.zone_name

        This supports one edge VM serving many clusters under:
            <cluster>.<resource_slug>.zone_name
        """
        if not edge_public_ip:
                raise ValueError("edge_public_ip is required")
        zone_name = (zone_name or "").strip().rstrip(".")
        if not zone_name:
                raise ValueError("zone_name is required")

        safe_resource = _sanitize_subdomain(resource_slug)

        apex = f"{safe_resource}.{zone_name}"
        wildcard = f"*.{safe_resource}.{zone_name}"

        upsert_a_record(zone_id=zone_id, api_token=api_token, fqdn=apex, ip=edge_public_ip, proxied=proxied)
        upsert_a_record(zone_id=zone_id, api_token=api_token, fqdn=wildcard, ip=edge_public_ip, proxied=proxied)
