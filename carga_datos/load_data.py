import json
import logging
import os
import sys

# Add the parent directory to sys.path to allow imports from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session

from config.database import SessionLocal, check_connection
from models.category import CategoryModel
from models.product import ProductModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CATEGORIES_FILE = os.path.join(DATA_DIR, "categories.json")
PRODUCTS_FILE = os.path.join(DATA_DIR, "products.json")


def load_categories(db: Session) -> dict:
    """
    Load categories from JSON file into the database.
    Returns a dictionary mapping category names to their IDs.
    """
    if not os.path.exists(CATEGORIES_FILE):
        logger.error(f"Categories file not found: {CATEGORIES_FILE}")
        return {}

    with open(CATEGORIES_FILE, 'r', encoding='utf-8') as f:
        categories_data = json.load(f)

    category_map = {}
    new_count = 0
    existing_count = 0

    logger.info(f"Processing {len(categories_data)} categories...")

    for cat_data in categories_data:
        name = cat_data.get("name")
        if not name:
            continue

        # Check if exists
        category = db.query(CategoryModel).filter(CategoryModel.name == name).first()
        
        if not category:
            category = CategoryModel(name=name)
            db.add(category)
            db.flush()  # Flush to get the ID without committing yet
            new_count += 1
        else:
            existing_count += 1
        
        category_map[name] = category.id

    db.commit()
    logger.info(f"Categories loaded: {new_count} new, {existing_count} existing.")
    return category_map


def load_products(db: Session, category_map: dict):
    """
    Load products from JSON file into the database.
    """
    if not os.path.exists(PRODUCTS_FILE):
        logger.error(f"Products file not found: {PRODUCTS_FILE}")
        return

    with open(PRODUCTS_FILE, 'r', encoding='utf-8') as f:
        products_data = json.load(f)

    new_count = 0
    skipped_count = 0

    logger.info(f"Processing {len(products_data)} products...")

    for prod_data in products_data:
        category_name = prod_data.get("category")
        
        if category_name not in category_map:
            logger.warning(f"Category '{category_name}' not found for product '{prod_data.get('name')}'. Skipping.")
            skipped_count += 1
            continue

        # Check if product exists (assuming uniqueness by name for simplicity in this script, 
        # though only ID is PK. Adjust logic if you want to update existing products)
        product_name = prod_data.get("name")
        existing_product = db.query(ProductModel).filter(ProductModel.name == product_name).first()

        if existing_product:
            # Optional: Update existing product? For now, we skip to avoid duplicates/errors
            # logger.info(f"Product '{product_name}' already exists. Skipping.")
            skipped_count += 1
            continue

        new_product = ProductModel(
            name=product_name,
            price=prod_data.get("price"),
            stock=prod_data.get("stock"),
            category_id=category_map[category_name]
        )
        db.add(new_product)
        new_count += 1

    db.commit()
    logger.info(f"Products loaded: {new_count} new, {skipped_count} skipped (existing or missing category).")


def main():
    logger.info("Starting data loading process...")
    
    if not check_connection():
        logger.error("Could not connect to the database. Exiting.")
        sys.exit(1)

    db = SessionLocal()
    try:
        category_map = load_categories(db)
        if category_map:
            load_products(db, category_map)
        else:
            logger.warning("No categories loaded. Skipping product loading.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        db.rollback()
    finally:
        db.close()
    
    logger.info("Data loading process completed.")


if __name__ == "__main__":
    main()
