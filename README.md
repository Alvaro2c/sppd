# SPPD Spanish Public Procurement Data

## Project Overview
This repository focuses on creating Python scripts and tools to efficiently download, parse, access, transform, and interact with Spanish public procurement data.

The official source of the data is the [Spanish Ministry of Finance](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx).

## Current Version

**Version:** 0.1.0


## Project Status

Currently the downloader and parser (dl_parser module) is fully functional

## Upcoming Features

- Database creation with raw data from parquet files
- Streamlit app to interact with the database (SQL and LLM)

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

To use dl_parser module, use the following command:
```sh
python src/dl_parser/main.py
```

## License

This project is licensed under the MIT License.

## Contact

For any questions or suggestions, please open an issue or contact the project maintainer.
