"""run_pipeline.py — runs bronze → silver → gold once."""
import logging, os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("pipeline")

from notebook_funcs import extract, transform, aggregate  # extract these from week7.ipynb

if __name__ == "__main__":
    try:
        log.info("=== pipeline start ===")
        bronze_key  = extract()
        silver_keys = transform(bronze_key)
        gold_key    = aggregate()
        log.info("=== pipeline OK: gold=%s ===", gold_key)
    except Exception as e:
        log.exception("pipeline FAILED: %s", e)
        raise
