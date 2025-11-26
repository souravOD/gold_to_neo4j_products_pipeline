import time
from typing import List

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.queue.outbox import fetch_pending_events, mark_failed, mark_processed
from src.adapters.supabase.db import PostgresPool
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.pipelines.product_pipeline import ProductPipeline
from src.utils.logging import configure_logging


def process_batch(pipeline: ProductPipeline, events: List[OutboxEvent], pg_pool: PostgresPool, log):
    for event in events:
        try:
            pipeline.handle_event(event)
            with pg_pool.connection() as conn:
                mark_processed(conn, event.id)
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed processing product event", extra={"event_id": event.id, "aggregate_id": event.aggregate_id})
            with pg_pool.connection() as conn:
                mark_failed(conn, event.id, str(exc))


def main():
    settings = Settings()
    log = configure_logging("product_worker")
    log.info("Starting product worker", extra={"pipeline": settings.pipeline_name})

    pg_pool = PostgresPool(settings.supabase_dsn)
    neo4j = Neo4jClient(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)
    pipeline = ProductPipeline(settings, pg_pool, neo4j)

    try:
        while True:
            with pg_pool.connection() as conn:
                conn.autocommit = False
                events = fetch_pending_events(
                    conn,
                    settings.batch_size,
                    settings.max_attempts,
                    table_names=[
                        "products",
                        "nutrition_facts",
                        "product_ingredients",
                        "product_categories",
                        "product_certifications",
                        "product_images",
                        "product_substitutions",
                        "product_age_restrictions",
                        "vendor_product_mappings",
                        "vendors",
                        "categories",
                        "certifications",
                        "age_bands",
                    ],
                    aggregate_types=["product"],
                )
                conn.commit()

            if not events:
                time.sleep(settings.poll_interval_seconds)
                continue

            process_batch(pipeline, events, pg_pool, log)
    finally:
        neo4j.close()
        pg_pool.close()


if __name__ == "__main__":
    main()
