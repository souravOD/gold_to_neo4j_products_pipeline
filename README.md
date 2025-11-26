# Pipeline: products

Scope: Ingest product-domain changes from Supabase Gold v3 into Neo4j v3 using a Python worker. This repo is self-contained (no shared code).

Supabase source tables: products, nutrition_facts (product), product_ingredients, product_categories, product_certifications, product_images, product_substitutions, product_age_restrictions, vendor_product_mappings, vendors, categories, certifications, age_bands
Neo4j labels touched: Product, ProductNutritionValue, Ingredient, Category, Certification, Vendor, AgeBand
Neo4j relationships touched: HAS_NUTRITION_VALUE, OF_NUTRIENT, CONTAINS_INGREDIENT, HAS_CATEGORY, HAS_CERTIFICATION, HAS_IMAGE, SUBSTITUTE_FOR, AGE_RESTRICTED_FOR, MAPPED_TO_GLOBAL, BELONGS_TO_VENDOR/OWNS_PRODUCT

How it works
- Outbox-driven: worker polls `outbox_events` (aggregate_type='product'), locks with `SKIP LOCKED`, routes to upsert.
- Upsert logic: reloads product core + nutrition_facts + ingredients + certifications + images + substitutions + age restrictions + vendor mappings; clears and replaces relationships idempotently.
- Deletes: if event op=DELETE and row absent in Supabase, `DETACH DELETE` the Product node; otherwise treat as upsert.

Run
- Install deps: `pip install -r requirements.txt`
- Configure env: copy `.env.example` → `.env` and fill Postgres/Neo4j credentials.
- Start worker: `python -m src.workers.runner`

Folders
- docs/: domain notes, Cypher patterns, event routing
- src/: config, adapters (supabase, neo4j, queue), domain models/services, pipelines (aggregate upserts), workers (runners), utils
- tests/: placeholder for unit/integration tests
- ops/: ops templates (docker/env/sample cron jobs)
