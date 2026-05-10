// Neo4j initial schema
CREATE CONSTRAINT vendor_id IF NOT EXISTS
  FOR (v:Vendor) REQUIRE v.vendor_id IS UNIQUE;

CREATE CONSTRAINT sku_id IF NOT EXISTS
  FOR (s:SKU) REQUIRE s.sku IS UNIQUE;

CREATE CONSTRAINT warehouse_id IF NOT EXISTS
  FOR (w:Warehouse) REQUIRE w.warehouse_id IS UNIQUE;

CREATE CONSTRAINT carrier_id IF NOT EXISTS
  FOR (c:Carrier) REQUIRE c.carrier_id IS UNIQUE;
