import logging
from src.ingestion import load_raw_csvs
from src.processing import DataCleaner, Categorizer
from src.analysis import save_to_db, generate_frontend_json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("Starting Financial Analysis Pipeline...")

    # 1. Ingestion
    raw_df = load_raw_csvs()
    if raw_df.empty:
        logger.error("Pipeline stopped: No data loaded.")
        return

    # 2. Processing
    cleaner = DataCleaner()
    categorizer = Categorizer()

    df = cleaner.clean(raw_df)
    df = categorizer.apply_categorization(df)

    # 3. Analysis & Export
    save_to_db(df)
    generate_frontend_json(df)

    logger.info("Pipeline completed successfully! Dashboard data is ready.")

if __name__ == "__main__":
    run_pipeline()
