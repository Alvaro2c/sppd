# SPPD Spanish Public Procurement Data

## Project Overview

This repository will allow users to:
- Fetch and parse
- Get insights from
- Interact with using a LLM

a database of public procurement data from Spain.

The official source of the data is the [Spanish Ministry of Finance](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx).

## Current Version

**Version:** 0.1.0

## Upcoming features

- Interactive querying of public procurement data.
- User-friendly interface powered by Streamlit.
- Integration with a Large Language Model for advanced data interaction.

## Project Status

Currently, there is a working notebook for parsing and database creation based on XML/ATOM files.

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
streamlit run src/app.py
```

## License

This project is licensed under the MIT License.

## Contact

For any questions or suggestions, please open an issue or contact the project maintainer.
