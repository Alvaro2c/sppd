[![CI](https://github.com/Alvaro2c/sppd/actions/workflows/build.yml/badge.svg)](https://github.com/Alvaro2c/sppd/actions/workflows/build.yml)

# SPPD Spanish Public Procurement Data

## Project Overview
This repository focuses on creating Python scripts and tools to efficiently download, parse, access, transform, and interact with Spanish public procurement data.

The official source of the data is the [Spanish Ministry of Finance](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx).

## Current Version

**Version:** 0.1.0


## Project Status

Currently the downloader and parser (dl_parser module) is fully functional.

## Upcoming Features

- Database creation with raw data from parquet files
- Streamlit app to interact with the database

## Getting Started

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/alvaro2c/sppd.git
    ```
2. Navigate to the project directory:
    ```sh
    cd sppd
    ```
3. Install the required packages:
    ```sh
    pip install -e .
    ```

### Using the modules


#### dl_parser

To use dl_parser module, use the following command:
```sh
python src/dl_parser/main.py
```

The dl_parser module follows these steps:
1. Fetches available data periods from the Spanish Ministry of Finance website
2. Prompts user to select:
   - Data period (year or month)
   - Duplicate handling strategy (by id, link, title, or keep all)
   - Whether to delete downloaded files after processing
3. Downloads and extracts ATOM files for the selected period
4. Processes XML data using predefined mappings to extract relevant fields:
   - Contract identifiers and basic info
   - Contracting party details
   - Procurement project information
   - Tendering process data
   - Results and winning party details
5. Concatenates all data into a single DataFrame
6. Handles duplicates according to chosen strategy
7. Saves final data as a parquet file


## License

This project is licensed under the MIT License.

## Contact

For any questions or suggestions, please open an issue or contact @alvaro2c.
