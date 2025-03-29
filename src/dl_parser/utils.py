# data processing
import pandas as pd

# web/xml scraping
import requests
import re
import xml.etree.ElementTree as ET

# local file handling
import zipfile
import io
import os
from tqdm import tqdm
import gc

# Common utils
from src.common.utils import get_soup, get_folder_path, get_full_paths
from src.dl_parser.mappings import mappings

from concurrent.futures import ProcessPoolExecutor
import time


def extract_digits_from_url(url):
    """
    Extracts the digits from the URL that correspond to the period of the file to be downloaded.

    Args:
        url (str): The URL from which to extract the period.

    Returns:
        str: The period extracted from (or None if not found in) the URL.
    """
    match = re.search(r"_(\d{4,6})\.zip", url)
    if match:
        return match.group(1)
    return None


def get_source_data(source_url: str):
    """
    Get the source data from the url and return a dict with the period as key and the link to the data as value.

    Args:
        source_url (str): The URL of the webpage to fetch the source data from.

    Returns:
        dict: A dictionary with the period as key and the link to the data as value.
    """

    soup = get_soup(source_url)
    links = [
        a.get("href")
        for a in soup.find_all("a")
        if "contratacion" in a.get("href") and a.get("href").endswith("zip")
    ]
    periods = [extract_digits_from_url(link) for link in links]
    source_data = {p: l for p, l in zip(periods, links)}

    return source_data


def recursive_field_dict(field, field_dict: dict):
    """
    Recursively converts an ATOM element and its children into a nested dictionary.

    Args:
        field (xml.etree.ElementTree.Element): The ATOM element to be converted.
        field_dict (dict): The dictionary to store the converted ATOM structure.

    Returns:
        None: The function modifies the field_dict in place to replicate the ATOM tree structure.
    """
    for child in field:
        tag = child.tag.split("}")[-1]
        if len(child) == 0:
            field_dict[tag] = child.text
        else:
            if tag not in field_dict:
                field_dict[tag] = {}
                recursive_field_dict(child, field_dict[tag])


def flatten_dict(d: dict, parent_key="", sep=".") -> dict:
    """
    Flattens a nested dictionary by concatenating keys with a specified separator.

    Args:
        d (dict): The dictionary to flatten.
        parent_key (str, optional): The base key to use for the flattened keys. Defaults to ''.
        sep (str, optional): The separator to use between concatenated keys. Defaults to '.'.

    Returns:
        dict: A new dictionary with flattened keys and their corresponding values.

    Example:
        >>> nested_dict = {'a': {'b': {'c': 1}}, 'd': 2}
        >>> flatten_dict(nested_dict)
        {'a.b.c': 1, 'd': 2}
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif v:
            items.append((new_key, v))
    return dict(items)


def get_atom_data(xml_file) -> tuple:
    """
    Parses an ATOM file and retrieves namespace information and entries.

    Args:
        xml_file (str): The path to the ATOM file to be parsed.

    Returns:
        tuple: A tuple containing:
            - entries (list): A list of ATOM elements found under the 'entry' tag in the default namespace.
            - atom (str): The default namespace URI enclosed in curly braces.
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    ns = {node[0]: node[1] for _, node in ET.iterparse(xml_file, events=["start-ns"])}
    atom = "{" + ns[""] + "}"

    for k, v in ns.items():
        try:
            ET.register_namespace(k, v)
        except ValueError:
            pass

    entries = root.findall(f"{atom}entry")

    return entries, ns


def get_data_list(entries: list, ns: dict) -> list:
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
            entry_data.update(flat_details)

        # Pop columns that are no longer needed
        pop_cols = ["summary", "ContractFolderStatus"]
        for pop_col in pop_cols:
            if pop_col in entry_data.keys():
                entry_data.pop(pop_col)

        data.append(entry_data)

    return data


def download_and_extract_zip(
    source_data: dict, period: str, data_path: str = "data/raw/atom"
):
    """
    This function receives a dictionary of source data per period and the selected period.
    It downloads the documents inside a folder named after the period.

    Parameters:
    source_data (dict): Dictionary with periods as keys and URLs as values.
    period (int): The selected period for which the data needs to be downloaded.
    data_path (str): The path to the data folder. Defaults to 'data/raw/atom'.
    """
    if period not in source_data.keys():
        raise ValueError(f"The period {period} is not available in the source data.")

    zip_url = source_data[period]
    folder = get_folder_path(period, data_path)
    print(f"\nRequesting zip file from {period}...")

    start_time = time.time()
    response = requests.get(zip_url)
    elapsed_time = time.time() - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    print(f"Download took {int(minutes)} minutes and {int(seconds)} seconds.")
    response.raise_for_status()  # Raise an error for bad status codes

    # Use an in-memory buffer for the zip file
    with io.BytesIO(response.content) as temp_file:
        with zipfile.ZipFile(temp_file) as atomzip:
            atom_files = atomzip.infolist()
            total_size = sum(file.file_size for file in atom_files)

            with tqdm(
                total=total_size, desc="Extracting", unit="B", unit_scale=True
            ) as pbar:
                for file in atom_files:
                    atomzip.extract(member=file, path=folder)
                    pbar.update(file.file_size)

    # Explicitly delete the response and buffer to free memory
    del response
    del temp_file

    # Run garbage collection to free memory
    gc.collect()

    files_in_folder = len(os.listdir(folder))
    print(f"{files_in_folder} ATOM files were downloaded.")


def _process_single_file(path):
    """Helper function to process a single ATOM file."""
    try:
        entries, ns = get_atom_data(path)
        data_list = get_data_list(entries, ns)
        return data_list if len(data_list) > 0 else None
    except Exception:
        return None


def _process_batch(paths_batch, batch_num, tmp_dir):
    """Helper function to process a batch of paths and save to parquet."""
    results = []
    failed_paths = []
    max_workers = max((os.cpu_count()) - 2, 1)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results_iter = executor.map(_process_single_file, paths_batch)

        for path, result in zip(paths_batch, results_iter):
            if result is not None:
                results.extend(result)
            else:
                failed_paths.append(path)

    if failed_paths:
        print(f"Failed to process {len(failed_paths)} files in batch {batch_num}")

    if results:
        df = pd.DataFrame(results)
        batch_file = os.path.join(tmp_dir, f"batch_{batch_num}.parquet")
        df.to_parquet(batch_file, index=False)
        return len(results)
    return 0


def get_concat_df(paths: list, raw_data_path: str) -> pd.DataFrame:
    """
    Process files in batches of 100, saving intermediate results as parquet files.

    Parameters:
    paths (list): A list with the full paths to the files with the data.

    Returns:
    pd.DataFrame: A pandas DataFrame with the data from all the files.
    """
    # Create temporary directory
    tmp_dir = os.path.join(raw_data_path, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    # Process in batches of 100
    batch_size = 100
    total_records = 0

    for i in tqdm(range(0, len(paths), batch_size), desc="Processing batches"):
        batch_paths = paths[i:i + batch_size]
        records = _process_batch(batch_paths, i // batch_size, tmp_dir)
        total_records += records

    if total_records == 0:
        raise ValueError("No files were successfully processed")

    # Read all parquet files and combine
    print("Combining all batches...")
    parquet_files = [os.path.join(tmp_dir, f) for f in os.listdir(tmp_dir) if f.endswith('.parquet')]
    final_df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

    # Cleanup temporary files
    for f in parquet_files:
        os.remove(f)
    os.rmdir(tmp_dir)

    return final_df


def get_full_parquet(
    period: str,
    dup_strategy: str = "None",
    apply_mapping: str = "N",
    raw_data_path: str = "data/raw",
) -> str:
    """
    Generates a full parquet file for the given period by processing and consolidating data.

    This function performs the following steps:
    1. Retrieves the folder path for the specified period.
    2. Obtains the full paths of files within the folder.
    3. Parses and concatenates the dataframes from the full paths.
    4. Removes duplicates based on the specified strategy.
    5. Optionally applies column mappings to the dataframe.
    6. Saves the processed dataframe as a single parquet file in the specified directory.

    Args:
        period (str): The period for which the parquet file is to be generated.
        dup_strategy (str): The strategy to use for removing duplicates: 'id', 'link', 'title', or 'None'.
        apply_mapping (str): Whether to apply column mappings ('Y' for yes, 'N' for no).
        data_path (str): The path to the data folder. Defaults to 'data/raw'.

    Returns:
        str: The path to the generated parquet file.
    """

    folder_parquet = get_folder_path("parquet", raw_data_path)
    os.makedirs(folder_parquet, exist_ok=True)

    # Define the parquet file path
    parquet_file = os.path.join(folder_parquet, f"{period}.parquet")

    # Get the source data
    folder = get_folder_path(f"atom/{period}", raw_data_path)
    full_paths = get_full_paths(folder)
    dfs = get_concat_df(full_paths, raw_data_path)

    dfs = remove_duplicates(dfs, dup_strategy)

    if apply_mapping == "Y":
        dfs = apply_mappings(dfs, mappings)

    # Save to parquet
    dfs.to_parquet(parquet_file, index=False, compression="snappy", engine="pyarrow")

    print(
        f"Parsed and created parquet file for {period} with {len(dfs)} rows and {dfs.shape[1]} columns."
    )

    return parquet_file


def remove_duplicates(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """
    Removes duplicates from a dataframe based on the 'link', 'id' or 'title' column.
    If there are duplicates, the most recent entry is kept.

    Args:
    df (pd.DataFrame): A pandas DataFrame.
    strategy (str): The strategy to use for removing duplicates. Allowed values are 'id', 'link', 'title' or 'None'.

    Returns:
    pd.DataFrame: A pandas DataFrame with duplicates removed with selected strategy.
    """

    if strategy == "None":
        return df

    strategies = ["id", "link", "title"]

    if strategy not in strategies:
        raise ValueError(
            f"Invalid strategy: {strategy}. Allowed strategies are {strategies}"
        )

    df["updated"] = pd.to_datetime(df["updated"])

    # Sort and drop duplicates
    no_dups_df = df.sort_values(["updated"], ascending=False).drop_duplicates(
        subset=[strategy], keep="first"
    )

    dups_dropped = len(df) - len(no_dups_df)
    print(f"{dups_dropped} duplicate rows (by {strategy}) were dropped.")

    return no_dups_df


def apply_mappings(df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """
    Applies mappings to the DataFrame columns based on the mappings dictionary.

    Args:
    df (pd.DataFrame): The DataFrame to which the mappings will be applied.
    mappings (dict): The dictionary with the mappings to be applied.

    Returns:
    pd.DataFrame: The DataFrame with the mappings applied.
    """
    selected_columns = {k: v for k, v in mappings.items() if k in df.columns}
    mapped_df = df[list(selected_columns.keys())].rename(columns=selected_columns)
    return mapped_df


def delete_files(period: str, data_path: str = "data/raw/atom"):
    """
    Deletes the folder with the given period name inside the data folder.

    Args:
    period (str): The name of the period whose folder is to be deleted.
    data_path (str): The path to the data folder. Defaults to 'data/raw/atom'.

    Returns:
    None
    """

    folder = get_folder_path(period, data_path)
    files_in_folder = len(os.listdir(folder))
    if os.path.exists(folder):
        os.rmdir(folder)
        print(f"{files_in_folder} ATOM files were deleted.")
    else:
        raise FileNotFoundError(f"The files for period {period} does not exist.")


def dl_parser(
    source_data: dict,
    selected_periods: str,
    dup_strategy: str,
    apply_mapping: str,
    del_files: str,
):
    """
    Main function to download, process, and save data for a specified period.

    Args:
    source_data (dict): Dictionary with periods as keys and URLs as values.
    selected_period (str): The period for which the data is to be processed.
    del_files (str): Whether to delete the downloaded files after processing.
    """

    for selected_period in selected_periods:
        download_and_extract_zip(source_data, selected_period)

    parquet_files = []
    for selected_period in selected_periods:
        parquet_file = get_full_parquet(
            period=selected_period,
            dup_strategy=dup_strategy,
            apply_mapping=apply_mapping,
        )
        parquet_files.append(parquet_file)

    if del_files == "Y":
        for selected_period in selected_periods:
            delete_files(selected_period)

    return parquet_files
