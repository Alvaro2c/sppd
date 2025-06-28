from src.dl_parser.utils import (
    get_source_data,
    download_and_extract_zip,
    get_folder_path,
    get_full_paths,
    get_atom_data,
    remove_duplicates,
    delete_files,
    recursive_field_dict,
    flatten_dict,
)
import polars as pl
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import os
import json
from datetime import datetime

"""
Open Tenders Data Processing Module

This module provides utilities for downloading, processing, and transforming open tenders
data from the Spanish government's open data portal. It handles ATOM feed processing,
parallel data extraction, code mapping, and data format conversion.

The module supports the complete pipeline from raw data download to processed JSON output,
including:
- Downloading recent open tenders data (last 3 months)
- Processing ATOM XML files in parallel batches
- Extracting and filtering relevant tender information
- Mapping codes to human-readable names using reference tables
- Converting data to various formats (parquet, JSON)

Key Functions:
- download_recent_data: Downloads the most recent 3 months of data
- open_tenders_process: Complete pipeline for data processing
- map_codes: Replaces codes with human-readable names
- save_mapped_data_to_json: Converts processed data to JSON format

Data Sources:
- Spanish Ministry of Finance open data portal
- ATOM feeds containing contract folder status information

Dependencies:
- polars: For efficient data processing
- concurrent.futures: For parallel processing
- tqdm: For progress tracking
- src.dl_parser.utils: For data download and parsing utilities
"""

source_url = "https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx"

data_path = "data/open_tenders"

open_tenders_cols = {
    "ContractFolderID": "ID",
    "ContractFolderStatusCode": "StatusCode",
    "LocatedContractingParty.Party.PartyName.Name": "ContractingParty",
    "LocatedContractingParty.Party.PostalAddress.CityName": "City",
    "LocatedContractingParty.Party.PostalAddress.Country.Name": "Country",
    "LocatedContractingParty.Party.PostalAddress.PostalZone": "ZipCode",
    "ProcurementProject.TypeCode": "ProjectTypeCode",
    "ProcurementProject.SubTypeCode": "ProjectSubTypeCode",
    "ProcurementProject.RequiredCommodityClassification.ItemClassificationCode": "CPVCode",
    "ProcurementProjectLot.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode": "CPVLotCode",
    "ProcurementProject.BudgetAmount.EstimatedOverallContractAmount": "EstimatedAmount",
    "ProcurementProject.BudgetAmount.TotalAmount": "TotalAmount",
    "ProcurementProject.BudgetAmount.TaxExclusiveAmount": "TaxExclusiveAmount",
    "TenderingProcess.ProcedureCode": "ProcessCode",
    "TenderingProcess.TenderSubmissionDeadlinePeriod.EndDate": "ProcessEndDate",
}


def download_recent_data(source_url: str, data_path: str):
    """
    Download the most recent 3 months of open tenders data from the source URL.

    This function fetches the source data dictionary, filters for the 3 most recent months
    based on the period codes, and downloads and extracts the ZIP files for each month.

    Args:
        source_url (str): The URL to fetch the source data from.
        data_path (str): The base path where data should be stored.

    Returns:
        list: A list of the last 3 months (period codes) that were downloaded.

    Example:
        >>> months = download_recent_data("https://example.com", "data/open_tenders")
        >>> print(months)
        ['202412', '202411', '202410']
    """
    source_dict = get_source_data(source_url)

    source_dict_recent = {
        k: v
        for k, v in source_dict.items()
        if len(k) > 4
        and k
        in sorted(
            [k for k in source_dict.keys() if len(k) > 4],
            key=lambda x: int(x[-2:]),
            reverse=True,
        )[:3]
    }

    last_months = source_dict_recent.keys()

    for month in last_months:
        download_and_extract_zip(
            source_data=source_dict_recent, period=month, data_path=f"{data_path}/raw"
        )

    return last_months


def get_data_list_open_tenders(entries: list, ns: dict) -> list:
    """
    Extracts the main information from the entries of the ATOM file and returns a list of dictionaries.

    Args:
        entries (list): List of ATOM elements representing the entries.
        ns (dict): Dictionary of namespaces used in the ATOM file.

    Returns:
        list: A list of dictionaries, where each dictionary contains the extracted data for an entry.
    """
    data = []

    for entry in entries:
        # Initialize entry data
        entry_data = {}

        # Extract general information
        for field in entry:
            tag = field.tag.split("}")[-1]
            entry_data[tag] = field.text if tag != "link" else field.get("href")

        # Generate full details information
        details = entry.find("cac-place-ext:ContractFolderStatus", ns)
        details_dict = {}

        if details is not None:
            recursive_field_dict(details, details_dict)
            flat_details = flatten_dict(details_dict)

        status = flat_details.get("ContractFolderStatusCode")
        end_date = flat_details.get(
            "TenderingProcess.TenderSubmissionDeadlinePeriod.EndDate"
        )

        # First exclude non-PUB status
        if status != "PUB":
            continue

        # Then for PUB status, check end date
        if (
            end_date is None
            or datetime.strptime(end_date, "%Y-%m-%d").date() <= datetime.now().date()
        ):
            continue

        else:
            filtered_details = {
                v: flat_details[k]
                for k, v in open_tenders_cols.items()
                if k in flat_details
            }

            entry_data.update(filtered_details)
            entry_data.pop("id")
            entry_data.pop("summary")
            entry_data.pop("ContractFolderStatus")

            # Cast monetary amounts to float32
            amount_fields = ["EstimatedAmount", "TotalAmount", "TaxExclusiveAmount"]
            for field in amount_fields:
                if field in entry_data:
                    try:
                        entry_data[field] = float(entry_data[field])
                    except (ValueError, TypeError):
                        entry_data[field] = None

            data.append(entry_data)

    return data


def process_single_file(path):
    """
    Process a single ATOM file and extract open tenders data.

    This helper function reads an ATOM file, extracts the entries and namespaces,
    processes them to extract open tenders information, and returns the data list.
    If processing fails or no data is found, it returns None.

    Args:
        path (str): The file path to the ATOM file to process.

    Returns:
        list or None: A list of dictionaries containing the extracted open tenders data,
                     or None if processing failed or no data was found.

    Example:
        >>> data = process_single_file("path/to/atom/file.xml")
        >>> if data:
        ...     print(f"Extracted {len(data)} records")
        ... else:
        ...     print("No data found or processing failed")
    """
    try:
        entries, ns = get_atom_data(path)
        data_list = get_data_list_open_tenders(entries, ns)
        return data_list if len(data_list) > 0 else None
    except Exception:
        return None


def process_batch(paths_batch, batch_num, tmp_dir):
    """
    Process a batch of ATOM files in parallel and save results to parquet.

    This helper function processes multiple ATOM files concurrently using a ProcessPoolExecutor,
    collects the results, and saves them to a temporary parquet file. It handles failed
    processing gracefully and reports the number of failed files.

    Args:
        paths_batch (list): List of file paths to process in this batch.
        batch_num (int): The batch number for naming the output file.
        tmp_dir (str): Directory path where temporary parquet files should be saved.

    Returns:
        int: The number of successfully processed records in this batch.

    Example:
        >>> records = process_batch(["file1.xml", "file2.xml"], 1, "/tmp/batches")
        >>> print(f"Processed {records} records in batch 1")
    """
    results = []
    null_paths = []
    max_workers = max((os.cpu_count()) - 2, 1)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results_iter = executor.map(process_single_file, paths_batch)

        for path, result in zip(paths_batch, results_iter):
            if result is not None:
                results.extend(result)
            else:
                null_paths.append(path)

    if null_paths:
        print(
            f"Processing gave null results for {len(null_paths)} files in batch {batch_num}"
        )

    if results:
        df = pl.DataFrame(results)
        batch_file = os.path.join(tmp_dir, f"batch_{batch_num}.parquet")
        df.write_parquet(batch_file, compression="snappy")
        return len(results)
    return 0


def get_parquet_open_tenders(paths: list, data_path: str, name="open_tenders") -> dict:
    """
    Process ATOM files in batches and create a consolidated parquet file.

    This function processes a large number of ATOM files by breaking them into batches
    of 100 files each. Each batch is processed in parallel and saved as a temporary
    parquet file. All temporary files are then combined into a single parquet file
    with duplicates removed based on the 'link' field.

    Args:
        paths (list): A list with the full paths to the ATOM files to process.
        data_path (str): The directory path where the final parquet file should be saved.
        name (str, optional): The name for the output parquet file (without extension).
                            Defaults to "open_tenders".

    Returns:
        dict: A dictionary containing:
            - 'parquet_path' (str): The full path to the created parquet file
            - 'df_shape' (tuple): The shape of the final dataframe (rows, columns)

    Raises:
        ValueError: If no files were successfully processed.

    Example:
        >>> result = get_parquet_open_tenders(file_paths, "data/open_tenders", "my_tenders")
        >>> print(f"File saved at: {result['parquet_path']}")
        >>> print(f"Data shape: {result['df_shape']}")
    """

    parquet_path = os.path.join(data_path, f"{name}.parquet")

    # Create temporary directory
    tmp_dir = os.path.join(data_path, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    # Process in batches of 100
    batch_size = 100
    total_records = 0

    for i in tqdm(
        range(0, len(paths), batch_size), desc="Processing batches", unit="batch"
    ):
        batch_paths = paths[i : i + batch_size]  # noqa: E203
        records = process_batch(batch_paths, i // batch_size, tmp_dir)
        total_records += records

    if total_records == 0:
        raise ValueError("No files were successfully processed")

    # Read all parquet files and combine
    print("Combining all batches...")
    parquet_files = [
        os.path.join(tmp_dir, f) for f in os.listdir(tmp_dir) if f.endswith(".parquet")
    ]
    final_df = pl.concat(
        [pl.scan_parquet(f) for f in parquet_files], how="diagonal"
    ).collect()

    final_df_no_dups = remove_duplicates(final_df, "link")

    final_df_no_dups.write_parquet(parquet_path, compression="snappy")

    # Cleanup temporary files
    for f in parquet_files:
        os.remove(f)
    os.rmdir(tmp_dir)

    return {"parquet_path": parquet_path, "df_shape": final_df_no_dups.shape}


def open_tenders_parquet(source_url: str, data_path: str, name="open_tenders_raw"):
    """
    Complete pipeline to download, process, and create parquet files for open tenders data.

    This function orchestrates the entire process of downloading recent open tenders data,
    processing the ATOM files, creating a consolidated parquet file, and cleaning up
    temporary files. It downloads the last 3 months of data, processes all files,
    and saves the results as a parquet file.

    Args:
        source_url (str): The URL to fetch the source data from.
        data_path (str): The base path where data should be stored and processed.
        name (str, optional): The name for the output parquet file (without extension).
                            Defaults to "open_tenders_raw".

    Returns:
        None: This function doesn't return a value but prints progress information
              and creates files in the specified data_path.

    Example:
        >>> open_tenders_process(
        ...     "https://example.com/data",
        ...     "data/open_tenders",
        ...     "my_tenders_raw"
        ... )
        # Downloads data, processes files, and creates parquet output
    """

    last_months = download_recent_data(source_url, data_path)

    folder = get_folder_path("raw", data_path)
    full_paths = [
        i for s in [get_full_paths(f) for f in get_full_paths(folder)] for i in s
    ]

    parquet_dict = get_parquet_open_tenders(full_paths, data_path, name)

    for month in last_months:
        delete_files(period=month, data_path=f"{data_path}/raw")

    print(
        f"Parquet file created at {parquet_dict['parquet_path']} with shape {parquet_dict['df_shape']}."
    )


def map_codes(
    data_path: str = "data/open_tenders",
    raw_name="open_tenders_raw",
    mapped_name="open_tenders",
) -> pl.DataFrame:
    """
    Replace codes with their corresponding names from reference tables.

    This function reads the raw open tenders parquet file and replaces various codes
    (ProcessCode, ProjectTypeCode, CPVCode, CPVLotCode, ProjectSubTypeCode) with their
    human-readable names using reference tables. It handles both simple mappings and
    conditional mappings for project subtypes based on the project type.

    Args:
        data_path (str, optional): Path to directory containing parquet files.
                                 Defaults to "data/open_tenders".
        raw_name (str, optional): Name of the raw parquet file (without extension).
                                Defaults to "open_tenders_raw".
        mapped_name (str, optional): Name for the output mapped parquet file (without extension).
                                   Defaults to "open_tenders".

    Returns:
        pl.DataFrame: The dataframe with codes replaced by their corresponding names.

    Note:
        This function expects the following reference parquet files to exist in data_path:
        - TenderingProcessCode.parquet
        - ContractCode.parquet
        - CPV2008.parquet
        - GoodsContractCode.parquet
        - ServiceContractCode.parquet
        - WorksContractCode.parquet
        - PatrimonialContractCode.parquet

    Example:
        >>> df = map_codes("data/open_tenders", "raw_data", "mapped_data")
        >>> print(f"Mapped {df.height} records")
        >>> print(df.head())
    """

    def make_map(filename, data_path):
        df = pl.read_parquet(f"{data_path}/{filename}.parquet")
        d = df.select(["code", "nombre"]).to_dict(as_series=False)
        return dict(zip(d["code"], d["nombre"]))

    # Mapping info: (column, parquet filename)
    mapping_info = [
        ("ProcessCode", "TenderingProcessCode"),
        ("ProjectTypeCode", "ContractCode"),
        ("CPVCode", "CPV2008"),
        ("CPVLotCode", "CPV2008"),
    ]

    # Create all mapping dicts
    maps = {col: make_map(parquet, data_path) for col, parquet in mapping_info}
    # Subtype maps
    subtype_maps = {
        "Suministros": make_map("GoodsContractCode", data_path),
        "Servicios": make_map("ServiceContractCode", data_path),
        "Obras": make_map("WorksContractCode", data_path),
        "Patrimonial": make_map("PatrimonialContractCode", data_path),
    }

    # Read open tenders data
    df = pl.read_parquet(f"{data_path}/{raw_name}.parquet")

    # Map simple columns
    for col in ["ProcessCode", "ProjectTypeCode", "CPVCode", "CPVLotCode"]:
        df = df.with_columns(
            [
                pl.col(col)
                .map_elements(lambda x, m=maps[col]: m.get(x, x), return_dtype=pl.Utf8)
                .alias(col)
            ]
        )

    # Conditional mapping for ProjectSubTypeCode
    def map_project_subtype(row):
        project_type = row["ProjectTypeCode"]
        sub_type_code = row["ProjectSubTypeCode"]
        m = subtype_maps.get(project_type)
        if m:
            return m.get(sub_type_code, sub_type_code)
        return sub_type_code

    df = df.with_columns(
        [
            pl.struct(["ProjectTypeCode", "ProjectSubTypeCode"])
            .map_elements(map_project_subtype, return_dtype=pl.Utf8)
            .alias("ProjectSubTypeCode")
        ]
    )

    # Save mapped data
    output_path = f"{data_path}/{mapped_name}.parquet"
    df.write_parquet(output_path)
    print(f"Mapped data saved to {output_path}")
    return df


def save_mapped_data_to_json(
    data_path: str = "data/open_tenders", name="open_tenders"
) -> str:
    """
    Convert mapped open tenders data to JSON format with metadata.

    This function reads the mapped open tenders parquet file and converts it to JSON format.
    The JSON file includes metadata (total records, columns, schema, creation timestamp,
    last updated timestamp) followed by the actual data records. Datetime values are
    converted to Unix timestamps for JSON compatibility.

    Args:
        data_path (str, optional): Path to directory containing parquet files.
                                 Defaults to "data/open_tenders".
        name (str, optional): Name of the mapped parquet file (without extension).
                            Defaults to "open_tenders".

    Returns:
        str: The full path to the created JSON file.

    Note:
        The JSON file structure is:
        {
            "metadata": {
                "total_records": int,
                "columns": list,
                "schema": dict,
                "created_at": int (unix timestamp),
                "last_updated": str (isoformat) or null
            },
            "data": [
                {record1},
                {record2},
                ...
            ]
        }

    Example:
        >>> json_path = save_mapped_data_to_json("data/open_tenders", "my_tenders")
        >>> print(f"JSON file saved at: {json_path}")
    """
    # Read mapped data
    df = pl.read_parquet(f"{data_path}/{name}.parquet")

    # Cast EstimatedAmount to float
    df = df.with_columns(pl.col("EstimatedAmount").cast(pl.Float64))

    # Create metadata dictionary with datetime converted to unix timestamps
    metadata = {
        "total_records": df.height,
        "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
        "created_at": int(datetime.now().timestamp()),
        "most_recent_update": (
            df.select(pl.col("updated")).max().item().isoformat()
            if df.select(pl.col("updated")).max().item()
            else None
        ),
        "earliest_update": (
            df.select(pl.col("updated")).min().item().isoformat()
            if df.select(pl.col("updated")).min().item()
            else None
        ),
        "total_estimated_amount": (
            df.select(pl.col("EstimatedAmount")).sum().item()
            if df.select(pl.col("EstimatedAmount")).sum().item()
            else 0
        ),
        "max_estimated_amount": (
            df.select(pl.col("EstimatedAmount")).max().item()
            if df.select(pl.col("EstimatedAmount")).max().item()
            else 0
        ),
        "min_estimated_amount": (
            df.select(pl.col("EstimatedAmount")).min().item()
            if df.select(pl.col("EstimatedAmount")).min().item()
            else 0
        ),
        "avg_estimated_amount": (
            df.select(pl.col("EstimatedAmount")).mean().item()
            if df.select(pl.col("EstimatedAmount")).mean().item()
            else 0
        ),
        "median_estimated_amount": (
            df.select(pl.col("EstimatedAmount")).median().item()
            if df.select(pl.col("EstimatedAmount")).median().item()
            else 0
        ),
    }

    # Save metadata and data separately
    json_path = f"{data_path}/{name}.json"

    with open(json_path, "w", encoding="utf-8") as f:
        # Write metadata first
        f.write('{\n"metadata":')
        json.dump(metadata, f, ensure_ascii=False, indent=2)

        # Write data records one by one, converting datetimes to unix timestamps
        f.write(',\n"data": [\n')
        for i, record in enumerate(df.iter_rows(named=True)):
            if i > 0:
                f.write(",\n")
            # Convert any datetime values to unix timestamps
            record_unix = {
                k: int(v.timestamp()) if hasattr(v, "timestamp") else v
                for k, v in record.items()
            }
            json.dump(record_unix, f, ensure_ascii=False)
        f.write("\n]}")

    print(f"Mapped data with metadata saved to {json_path}")
    return json_path
