# SPPD Spanish Public Procurement Data

## Project Overview

This repository will allow users to:
- Fetch and parse public procurement data from Spain in atom/xml format and build parquet files from the raw data.
- Create a SQL database from the parquet files.
- Build an app with Streamlit to interact with the database.

The official source of the data is the [Spanish Ministry of Finance](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx).

## Current Version

**Version:** 0.1.0

## Upcoming Features

- Interactive querying of public procurement data.
- User-friendly interface powered by Streamlit.
- Integration with a Large Language Model for advanced data interaction.

## Project Status

Currently, the download and parser are working. The rest of the features are still in progress.

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

### Running the App

To run the Streamlit app, use the following command:
```sh
streamlit run src/app/app.py
```

## License

This project is licensed under the MIT License.

## Contact

For any questions or suggestions, please open an issue or contact the project maintainer.
