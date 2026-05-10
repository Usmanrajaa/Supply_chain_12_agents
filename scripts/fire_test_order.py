"""Fire a sample order against the local API. Run inside any container:
    docker compose exec api python -m scripts.fire_test_order
"""
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx


async def main() -> None:
    samples = [
        # Standard SKU with stock — happy path
        {"sku": "SKU-001", "quantity": 5, "total_value": 250.00, "label": "happy path"},
        # SKU with low stock — triggers vendor agent
        {"sku": "SKU-002", "quantity": 10, "total_value": 100.00, "label": "stock low → vendor"},
        # MFG SKU with no stock — triggers production agent
        {"sku": "MFG-001", "quantity": 3, "total_value": 600.00, "label": "stock low → production"},
        # High-value order — triggers approvals
        {"sku": "SKU-001", "quantity": 1, "total_value": 15000.00, "label": "high-value → approvals"},
    ]

    async with httpx.AsyncClient(base_url="http://api:8000", timeout=10) as client:
        for s in samples:
            payload = {
                "customer_id": str(uuid4()),
                "sku": s["sku"],
                "quantity": s["quantity"],
                "total_value": s["total_value"],
                "deadline": (datetime.now(timezone.utc) + timedelta(days=7)).isoformat(),
            }
            r = await client.post("/orders", json=payload)
            print(f"[{s['label']:30s}] {r.status_code} {r.json()}")


if __name__ == "__main__":
    asyncio.run(main())
