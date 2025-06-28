#!/usr/bin/env python3
"""
Open Tenders Data Processing Pipeline

This script orchestrates the complete open tenders data processing pipeline for Spanish
public procurement data. It downloads recent open tenders data, processes it, maps codes
to human-readable names, and saves the results in multiple formats.

The pipeline consists of four main steps:
1. Download and process raw open tenders data (last 3 months)
2. Download and prepare reference code tables (codices)
3. Map codes to human-readable names using reference tables
4. Save the processed data in JSON format

Data Sources:
- Spanish Ministry of Finance open data portal
- Contracting State Code (Código de Contratación del Estado)

Output:
- Raw data saved as parquet files
- Processed data with mapped codes saved as parquet and JSON files
- Reference code tables saved as parquet files

Usage:
    python src/open_tenders/main.py

Requirements:
    - Internet connection for data download
    - Sufficient disk space for data storage
    - Required Python packages (see requirements.txt)
"""

import logging
from pathlib import Path
from datetime import datetime
from src.open_tenders.utils import (
    open_tenders_parquet,
    map_codes,
    save_mapped_data_to_json,
)
from src.db_init.utils import get_db_codice_tables


def setup_logging():
    """
    Set up simple logging to stdout with timestamp and level.
    """
    logger = logging.getLogger("open_tenders_pipeline")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def main():
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("OPEN TENDERS DATA PROCESSING PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    source_url = "https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx"
    codice_url = "https://contrataciondelestado.es/codice/cl/"
    data_path = "data/open_tenders"

    try:
        logger.info("Step 1/4: Downloading and processing raw open tenders data...")
        open_tenders_parquet(
            source_url=source_url, data_path=data_path, name="open_tenders_raw"
        )
        logger.info("✓ Raw data processing completed successfully")

        logger.info("Step 2/4: Downloading reference code tables (codices)...")
        get_db_codice_tables(
            codice_url=codice_url, data_path=data_path, with_mappings=True, mapping="ot"
        )
        logger.info("✓ Reference code tables downloaded successfully")

        logger.info("Step 3/4: Mapping codes to human-readable names...")
        map_codes(
            data_path=data_path, raw_name="open_tenders_raw", mapped_name="open_tenders"
        )
        logger.info("✓ Code mapping completed successfully")

        logger.info("Step 4/4: Saving processed data in JSON format...")
        json_path = save_mapped_data_to_json(data_path=data_path, name="open_tenders")
        logger.info(f"✓ Data saved to JSON: {json_path}")

        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("Output files:")
        logger.info(f"  - Raw data: {data_path}/open_tenders_raw.parquet")
        logger.info(f"  - Processed data: {data_path}/open_tenders.parquet")
        logger.info(f"  - JSON output: {json_path}")
        logger.info(f"  - Reference tables: {data_path}/*.parquet")
        logger.info(
            "The open tenders data processing pipeline has completed successfully!"
        )

    except Exception as e:
        logger.error("=" * 80)
        logger.error("PIPELINE FAILED")
        logger.error("=" * 80)
        logger.error(
            f"Error occurred at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.error(f"Error: {str(e)}")
        logger.error("Please check the error message above and ensure:")
        logger.error("  - Internet connection is available")
        logger.error("  - Sufficient disk space is available")
        logger.error("  - All required packages are installed")
        logger.error("  - Data source URLs are accessible")
        import sys

        sys.exit(1)


if __name__ == "__main__":
    main()
