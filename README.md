[![CI](https://github.com/Alvaro2c/sppd/actions/workflows/build.yml/badge.svg)](https://github.com/Alvaro2c/sppd/actions/workflows/build.yml)

# SPPD Spanish Public Procurement Data

## Project Overview
Python tools for downloading, processing, and analyzing Spanish public procurement data from the [Spanish Ministry of Finance](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx).

## Current Version

**Version:** 0.3.0

## Project Status

### ✅ Implemented Features

- **Data Downloader & Parser**: Parallel and batch processing for historical data
- **Open Tenders Pipeline**: Automated daily processing of recent open tenders
- **GitHub Actions Workflow**: Automated data pipeline with cross-repository updates
- **Code Mapping**: Human-readable field mapping for procurement codes
- **Multiple Output Formats**: Parquet and JSON data exports

### 📊 Data Processing

- **Historical Data**: Full download and processing of past procurement data
- **Open Tenders**: Daily updates of current open tenders (last 3 months)
- **Code Tables**: Reference mappings for procurement categories and regions
- **Data Validation**: URL verification and output file validation

## Quick Start

### Installation

```sh
git clone https://github.com/alvaro2c/sppd.git
cd sppd
pip install -e .
```

### Usage

#### Historical Data Processing
```sh
python src/dl_parser/main.py
```

#### Open Tenders Pipeline
```sh
python src/open_tenders/main.py
```

#### Automated Workflow
- Configure GitHub Actions secrets
- Run manually or let it execute daily at 8:30 AM CET
- Updates target repository automatically

## Project Structure

```
sppd/
├── src/
│   ├── dl_parser/          # Historical data processing
│   ├── open_tenders/       # Open tenders pipeline
│   └── db_init/            # Database initialization
├── data/                   # Processed data output
├── .github/workflows/      # Automated pipelines
└── requirements.txt        # Dependencies
```

## License

MIT License - see LICENSE file for details.

## Contact

For questions or suggestions, open an issue or contact @alvaro2c.
