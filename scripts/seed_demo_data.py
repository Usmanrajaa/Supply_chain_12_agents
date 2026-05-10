"""Seed Neo4j demo data. Postgres seed runs automatically from migrations/postgres/001_init.sql.

Usage (from inside any app container):
    docker compose exec api python -m scripts.seed_demo_data
"""
import asyncio

from common.storage.neo4j_client import get_driver, close_driver


NEO4J_SEED = """
MERGE (v1:Vendor {vendor_id: '11111111-1111-1111-1111-111111111111'})
  SET v1.name = 'Acme Components', v1.rating = 4.7
MERGE (v2:Vendor {vendor_id: '22222222-2222-2222-2222-222222222222'})
  SET v2.name = 'Bharat Supplies', v2.rating = 4.3
MERGE (s1:SKU {sku: 'SKU-001'})
MERGE (s2:SKU {sku: 'SKU-002'})
MERGE (s3:SKU {sku: 'SKU-003'})
MERGE (sm:SKU {sku: 'MFG-001'})
MERGE (v1)-[:SUPPLIES {lead_time_h: 48, unit_price: 12.50}]->(s1)
MERGE (v2)-[:SUPPLIES {lead_time_h: 24, unit_price:  8.75}]->(s2)
MERGE (v1)-[:SUPPLIES {lead_time_h: 48, unit_price: 15.00}]->(s3)
MERGE (wh:Warehouse {warehouse_id: 'WH-DEL'})
  SET wh.name = 'Delhi'
MERGE (wh)-[:STOCKS]->(s1)
MERGE (wh)-[:STOCKS]->(s2)
MERGE (wh)-[:STOCKS]->(sm)
"""


async def main() -> None:
    driver = get_driver()
    async with driver.session() as s:
        await s.run(NEO4J_SEED)
    await close_driver()
    print("✓ Neo4j seeded")
    print("  (Postgres data was loaded automatically from migrations/postgres/001_init.sql)")


if __name__ == "__main__":
    asyncio.run(main())
