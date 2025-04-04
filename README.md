[![CI](https://github.com/Alvaro2c/sppd/actions/workflows/build.yml/badge.svg)](https://github.com/Alvaro2c/sppd/actions/workflows/build.yml)

# SPPD Spanish Public Procurement Data

## Project Overview
This repository focuses on creating Python scripts and tools to efficiently download, parse, access, transform, and interact with Spanish public procurement data.

The official source of the data is the [Spanish Ministry of Finance](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx).

## Current Version

**Version:** 0.2.0


## Project Status

Currently the downloader and parser (dl_parser module) is fully functional, now implemented with parallel and batch processing for speed, stability and scalability.

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
   - Data periods (year or month)
   - Duplicate handling strategy (by id, link, title, or keep all)
   - Mapping application (to extract relevant fields only)
   - Whether to delete downloaded files after processing
3. Downloads and extracts ATOM files for the selected period(s)
4. Processes ATOM data (batch and parallel processing)
5. Concatenates all data into a single DataFrame
6. Handles duplicates according to chosen strategy (select None if you want full raw data)
7. Aplies mapping if selected (do not selected if you want full raw data)
8. Saves final data as a parquet file


## License

This project is licensed under the MIT License.

## Contact

For any questions or suggestions, please open an issue or contact @alvaro2c.
