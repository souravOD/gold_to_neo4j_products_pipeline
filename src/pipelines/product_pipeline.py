from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class ProductPipeline:
    """Upserts Product aggregates (product + nutrition + ingredients + aux) into Neo4j."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("product_pipeline")

    # ===================== DATA LOADERS =====================
    def load_product(self, conn, product_id: str) -> Optional[Dict]:
        sql = """
        SELECT p.*, v.name AS vendor_name, v.slug AS vendor_slug, v.vendor_type,
               pc.id AS category_id, pc.name AS category_name, pc.slug AS category_slug
        FROM products p
        LEFT JOIN vendors v ON v.id = p.vendor_id
        LEFT JOIN product_categories pc ON pc.id = p.category_id
        WHERE p.id = %s;
        """
        return pg.fetch_one(conn, sql, (product_id,))

    def load_nutrition_facts(self, conn, product_id: str) -> List[Dict]:
        sql = """
        SELECT nf.id, nf.nutrient_id, nf.amount, nf.unit, nf.per_amount, nf.per_amount_grams,
               nf.percent_daily_value, nf.data_source, nf.confidence_score, nf.measurement_date
        FROM nutrition_facts nf
        WHERE nf.entity_type = 'product' AND nf.entity_id = %s;
        """
        return pg.fetch_all(conn, sql, (product_id,))

    def load_ingredients(self, conn, product_id: str) -> List[Dict]:
        sql = """
        SELECT pi.ingredient_id AS id, i.name, pi.quantity, pi.unit,
               pi.quantity_normalized_g, pi.ingredient_order, pi.is_primary
        FROM product_ingredients pi
        LEFT JOIN ingredients i ON i.id = pi.ingredient_id
        WHERE pi.product_id = %s
        ORDER BY pi.ingredient_order NULLS LAST;
        """
        return pg.fetch_all(conn, sql, (product_id,))

    def load_certifications(self, conn, product_id: str) -> List[Dict]:
        sql = """
        SELECT pc.certification_id AS id, c.code, c.name,
               pc.certification_date, pc.expiry_date, pc.certifying_body
        FROM product_certifications pc
        JOIN certifications c ON c.id = pc.certification_id
        WHERE pc.product_id = %s;
        """
        return pg.fetch_all(conn, sql, (product_id,))

    def load_images(self, conn, product_id: str) -> List[Dict]:
        sql = """
        SELECT id, image_url, image_type, image_order, alt_text
        FROM product_images
        WHERE product_id = %s
        ORDER BY image_order NULLS LAST;
        """
        return pg.fetch_all(conn, sql, (product_id,))

    def load_substitutions(self, conn, product_id: str) -> List[Dict]:
        sql = """
        SELECT substitute_product_id AS id, substitution_reason, dietary_compatibility,
               price_difference, confidence_score
        FROM product_substitutions
        WHERE original_product_id = %s;
        """
        return pg.fetch_all(conn, sql, (product_id,))

    def load_age_restrictions(self, conn, product_id: str) -> List[Dict]:
        sql = """
        SELECT par.age_band_id AS id, ab.code, ab.min_age_months, ab.max_age_months,
               par.restriction_type, par.reason, par.guideline_source
        FROM product_age_restrictions par
        JOIN age_bands ab ON ab.id = par.age_band_id
        WHERE par.product_id = %s;
        """
        return pg.fetch_all(conn, sql, (product_id,))

    def load_vendor_mappings(self, conn, product_id: str) -> List[Dict]:
        sql = """
        SELECT vendor_product_id, vendor_id, confidence_score, mapping_method
        FROM vendor_product_mappings
        WHERE global_product_id = %s;
        """
        return pg.fetch_all(conn, sql, (product_id,))

    # ===================== CYPHER =====================
    def _upsert_cypher(self) -> str:
        """Cypher script to rebuild all product relationships idempotently."""
        return """
        MERGE (p:Product {id: $product.id})
        SET p.name = $product.name,
            p.brand = $product.brand,
            p.description = $product.description,
            p.barcode = $product.barcode,
            p.gtin_type = $product.gtin_type,
            p.price = $product.price,
            p.currency = $product.currency,
            p.package_weight = $product.package_weight,
            p.package_weight_g = $product.package_weight_g,
            p.serving_size = $product.serving_size,
            p.serving_size_g = $product.serving_size_g,
            p.servings_per_container = $product.servings_per_container,
            p.image_url = $product.image_url,
            p.product_url = $product.product_url,
            p.manufacturer = $product.manufacturer,
            p.country_of_origin = $product.country_of_origin,
            p.status = $product.status,
            p.vendor_specific_attrs = $product.vendor_specific_attrs,
            p.external_id = $product.external_id,
            p.global_product_id = $product.global_product_id,
            p.calories = $product.calories,
            p.calories_from_fat = $product.calories_from_fat,
            p.total_fat_g = $product.total_fat_g,
            p.saturated_fat_g = $product.saturated_fat_g,
            p.trans_fat_g = $product.trans_fat_g,
            p.polyunsaturated_fat_g = $product.polyunsaturated_fat_g,
            p.monounsaturated_fat_g = $product.monounsaturated_fat_g,
            p.cholesterol_mg = $product.cholesterol_mg,
            p.sodium_mg = $product.sodium_mg,
            p.total_carbs_g = $product.total_carbs_g,
            p.dietary_fiber_g = $product.dietary_fiber_g,
            p.total_sugars_g = $product.total_sugars_g,
            p.added_sugars_g = $product.added_sugars_g,
            p.sugar_alcohols_g = $product.sugar_alcohols_g,
            p.protein_g = $product.protein_g,
            p.vitamin_a_mcg = $product.vitamin_a_mcg,
            p.vitamin_c_mg = $product.vitamin_c_mg,
            p.vitamin_d_mcg = $product.vitamin_d_mcg,
            p.calcium_mg = $product.calcium_mg,
            p.iron_mg = $product.iron_mg,
            p.potassium_mg = $product.potassium_mg,
            p.updated_at = datetime($product.updated_at),
            p.created_at = datetime($product.created_at)

        // Vendor ownership
        WITH p
        OPTIONAL MATCH (p)<-[oldVendor:OWNS_PRODUCT]-(:Vendor)
        DELETE oldVendor;
        WITH p
        CALL {
          WITH p
          OPTIONAL MATCH (v:Vendor {id: $product.vendor_id})
          FOREACH (ignore IN CASE WHEN v IS NULL THEN [] ELSE [1] END |
            MERGE (v)-[:OWNS_PRODUCT]->(p)
            SET v.name = coalesce($product.vendor_name, v.name),
                v.slug = coalesce($product.vendor_slug, v.slug),
                v.vendor_type = coalesce($product.vendor_type, v.vendor_type)
          )
          RETURN 0
        }

        // Category
        WITH p
        OPTIONAL MATCH (p)-[oldCat:HAS_CATEGORY]->(:Category)
        DELETE oldCat;
        WITH p
        FOREACH (_ IN CASE WHEN $product.category_id IS NULL THEN [] ELSE [1] END |
          MERGE (c:Category {id: $product.category_id})
          SET c.name = $product.category_name,
              c.slug = $product.category_slug
          MERGE (p)-[:HAS_CATEGORY]->(c)
        )

        // Ingredients
        WITH p
        OPTIONAL MATCH (p)-[oldIng:CONTAINS_INGREDIENT]->(:Ingredient)
        DELETE oldIng;
        WITH p, $ingredients AS ingredients
        UNWIND ingredients AS ing
          MERGE (i:Ingredient {id: ing.id})
          SET i.name = coalesce(ing.name, i.name)
          MERGE (p)-[ri:CONTAINS_INGREDIENT]->(i)
          SET ri.quantity = ing.quantity,
              ri.unit = ing.unit,
              ri.quantity_normalized_g = ing.quantity_normalized_g,
              ri.ingredient_order = ing.ingredient_order,
              ri.is_primary = ing.is_primary;

        // Certifications
        WITH p
        OPTIONAL MATCH (p)-[oldCert:HAS_CERTIFICATION]->(:Certification)
        DELETE oldCert;
        WITH p, $certifications AS certs
        UNWIND certs AS cert
          MERGE (c:Certification {id: cert.id})
          SET c.code = cert.code,
              c.name = cert.name
          MERGE (p)-[rc:HAS_CERTIFICATION]->(c)
          SET rc.certification_date = cert.certification_date,
              rc.expiry_date = cert.expiry_date,
              rc.certifying_body = cert.certifying_body;

        // Images
        WITH p
        OPTIONAL MATCH (p)-[oldImg:HAS_IMAGE]->(:Image)
        DELETE oldImg;
        WITH p, $images AS imgs
        UNWIND imgs AS img
          MERGE (im:Image {id: img.id})
          SET im.url = img.image_url,
              im.image_type = img.image_type,
              im.image_order = img.image_order,
              im.alt_text = img.alt_text
          MERGE (p)-[:HAS_IMAGE]->(im);

        // Age restrictions
        WITH p
        OPTIONAL MATCH (p)-[oldAge:AGE_RESTRICTED_FOR]->(:AgeBand)
        DELETE oldAge;
        WITH p, $age_restrictions AS ages
        UNWIND ages AS a
          MERGE (ab:AgeBand {id: a.id})
          SET ab.code = a.code,
              ab.min_age_months = a.min_age_months,
              ab.max_age_months = a.max_age_months
          MERGE (p)-[ar:AGE_RESTRICTED_FOR]->(ab)
          SET ar.restriction_type = a.restriction_type,
              ar.reason = a.reason,
              ar.guideline_source = a.guideline_source;

        // Substitutions
        WITH p
        OPTIONAL MATCH (p)-[oldSub:SUBSTITUTE_FOR]->(:Product)
        DELETE oldSub;
        WITH p, $substitutions AS subs
        UNWIND subs AS s
          MERGE (sp:Product {id: s.id})
          MERGE (p)-[rs:SUBSTITUTE_FOR]->(sp)
          SET rs.substitution_reason = s.substitution_reason,
              rs.dietary_compatibility = s.dietary_compatibility,
              rs.price_difference = s.price_difference,
              rs.confidence_score = s.confidence_score;

        // Vendor product mapping (vendor-specific product -> global product)
        WITH p
        OPTIONAL MATCH (:Product)-[oldMap:MAPPED_TO_GLOBAL]->(p)
        DELETE oldMap;
        WITH p, $vendor_mappings AS mappings
        UNWIND mappings AS vm
          MERGE (vp:Product {id: vm.vendor_product_id})
          SET vp.source = 'vendor',
              vp.vendor_id = vm.vendor_id
          MERGE (v:Vendor {id: vm.vendor_id})
          MERGE (v)-[:OWNS_PRODUCT]->(vp)
          MERGE (vp)-[map:MAPPED_TO_GLOBAL]->(p)
          SET map.confidence_score = vm.confidence_score,
              map.mapping_method = vm.mapping_method;

        // Nutrition values
        WITH p
        OPTIONAL MATCH (p)-[oldNv:HAS_NUTRITION_VALUE]->(nv:ProductNutritionValue)
        DELETE oldNv, nv;
        WITH p, $nutrition_facts AS facts
        UNWIND facts AS nf
          MATCH (nd:NutrientDefinition {id: nf.nutrient_id})
          MERGE (nv:ProductNutritionValue {id: nf.id})
          SET nv.amount = nf.amount,
              nv.unit = nf.unit,
              nv.per_amount = nf.per_amount,
              nv.per_amount_grams = nf.per_amount_grams,
              nv.percent_daily_value = nf.percent_daily_value,
              nv.data_source = nf.data_source,
              nv.confidence_score = nf.confidence_score,
              nv.measurement_date = nf.measurement_date
          MERGE (p)-[:HAS_NUTRITION_VALUE]->(nv)
          MERGE (nv)-[:OF_NUTRIENT]->(nd);
        """

    def _delete_cypher(self) -> str:
        return "MATCH (p:Product {id: $id}) DETACH DELETE p;"

    # ===================== OPERATIONS =====================
    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            product = self.load_product(conn, event.aggregate_id)

        if product is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting product from graph", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning(
                    "Product missing in Supabase; skipping upsert",
                    extra={"id": event.aggregate_id, "op": event.op},
                )
            return

        with self.pg_pool.connection() as conn:
            nutrition = self.load_nutrition_facts(conn, event.aggregate_id)
            ingredients = self.load_ingredients(conn, event.aggregate_id)
            certifications = self.load_certifications(conn, event.aggregate_id)
            images = self.load_images(conn, event.aggregate_id)
            substitutions = self.load_substitutions(conn, event.aggregate_id)
            age_restrictions = self.load_age_restrictions(conn, event.aggregate_id)
            vendor_mappings = self.load_vendor_mappings(conn, event.aggregate_id)

        params = {
            "product": product,
            "nutrition_facts": nutrition,
            "ingredients": ingredients,
            "certifications": certifications,
            "images": images,
            "substitutions": substitutions,
            "age_restrictions": age_restrictions,
            "vendor_mappings": vendor_mappings,
        }
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info(
            "Upserted product aggregate",
            extra={
                "id": event.aggregate_id,
                "nutrition_values": len(nutrition),
                "ingredients": len(ingredients),
                "certifications": len(certifications),
            },
        )
