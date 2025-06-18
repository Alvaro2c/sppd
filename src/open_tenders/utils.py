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

        if details:
            recursive_field_dict(details, details_dict)
            flat_details = flatten_dict(details_dict)

        status = flat_details.get("ContractFolderStatusCode")
        if status != "PUB":
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

            data.append(entry_data)

    return data


def process_single_file(path):
    """Helper function to process a single ATOM file."""
    try:
        entries, ns = get_atom_data(path)
        data_list = get_data_list_open_tenders(entries, ns)
        return data_list if len(data_list) > 0 else None
    except Exception:
        return None


def process_batch(paths_batch, batch_num, tmp_dir):
    """Helper function to process a batch of paths and save to parquet."""
    results = []
    failed_paths = []
    max_workers = max((os.cpu_count()) - 2, 1)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results_iter = executor.map(process_single_file, paths_batch)

        for path, result in zip(paths_batch, results_iter):
            if result is not None:
                results.extend(result)
            else:
                failed_paths.append(path)

    if failed_paths:
        print(f"Failed to process {len(failed_paths)} files in batch {batch_num}")

    if results:
        df = pl.DataFrame(results)
        batch_file = os.path.join(tmp_dir, f"batch_{batch_num}.parquet")
        df.write_parquet(batch_file, compression="snappy")
        return len(results)
    return 0


def get_parquet_open_tenders(paths: list, data_path: str, name="open_tenders") -> dict:
    """
    Process files in batches of 100, saving intermediate results as parquet files.

    Parameters:
    paths (list): A list with the full paths to the files with the data.

    Returns:
    parquet_dict (dict): Dict with parquet path and df shape.
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
    final_df = pl.concat([pl.read_parquet(f) for f in parquet_files], how="diagonal")

    final_df_no_dups = remove_duplicates(final_df, "link")

    final_df_no_dups.write_parquet(parquet_path, compression="snappy")

    # Cleanup temporary files
    for f in parquet_files:
        os.remove(f)
    os.rmdir(tmp_dir)

    return {"parquet_path": parquet_path, "df_shape": final_df_no_dups.shape}


def open_tenders_process(source_url: str, data_path: str, name="open_tenders"):

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
