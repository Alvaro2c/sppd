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

# Common utils
from src.common.utils import get_soup, get_folder_path, get_full_paths
from src.dl_parser.mappings import mappings


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


def get_df(entries: list, ns: dict) -> pd.DataFrame:
    """
    Extracts the main information from the entries of the ATOM file and returns a DataFrame with the data.

    Parameters:
    entries (list): List of ATOML elements representing the entries.
    ns (dict): Dictionary of namespaces used in the ATOM file.
    mapping (dict): Dictionary mapping the desired DataFrame column names to the corresponding ATOM tags.

    Returns:
    pd.DataFrame: DataFrame containing the extracted data.
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

    df = pd.DataFrame(data)

    return df


def download_and_extract_zip(source_data: dict, period: str, data_path: str = "data"):
    """
    This function receives a dictionary of source data per period and the selected period.
    It downloads the documents inside a folder named after the period.

    Parameters:
    source_data (dict): Dictionary with periods as keys and URLs as values.
    period (int): The selected period for which the data needs to be downloaded.
    data_path (str): The path to the data folder. Defaults to 'data'.
    """
    if period not in source_data.keys():
        raise ValueError(f"The period {period} is not available in the source data.")
    else:
        zip_url = source_data[period]
        folder = get_folder_path(period, data_path)
        print(f"Requesting zip file from {period}...")
        response = requests.get(zip_url)
        with zipfile.ZipFile(io.BytesIO(response.content)) as atomzip:
            atom_files = atomzip.infolist()
            total_size = sum(file.file_size for file in atom_files)

            with tqdm(
                total=total_size, desc="Extracting", unit="B", unit_scale=True
            ) as pbar:
                for file in atom_files:
                    atomzip.extract(member=file, path=folder)
                    pbar.update(file.file_size)

        files_in_folder = len(os.listdir(folder))
        print(f"{files_in_folder} ATOM files were downloaded.")


def get_concat_dfs(paths: list) -> pd.DataFrame:
    """
    This function receives a list of full paths to the files with the data.
    It returns a DataFrame with the data from all the files.

    Parameters:
    paths (list): A list with the full paths to the files with the data.

    Returns:
    DataFrame: A DataFrame with the data from all the files.
    """
    dfs = []
    with tqdm(total=len(paths), desc="Parsing files", unit="file") as pbar:
        for path in paths:
            entries, ns = get_atom_data(path)
            df = get_df(entries, ns)
            dfs.append(df)
            pbar.update(1)

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def get_full_parquet(
    period: str,
    dup_strategy: str = "None",
    apply_mapping: str = "N",
    data_path: str = "data",
) -> str:
    """
    Generates a full parquet file for the given period by processing and consolidating data.

    This function performs the following steps:
    1. Retrieves the folder path for the specified period.
    2. Obtains the full paths of files within the folder.
    3. Parses and concatenates the dataframes from the full paths.
    4. Removes duplicates based on the specified strategy.
    5. Optionally applies column mappings to the dataframe.
    6. Saves the processed dataframe as a parquet file in the specified directory.

    Args:
        period (str): The period for which the parquet file is to be generated.
        dup_strategy (str): The strategy to use for removing duplicates: 'id', 'link', 'title', or 'None'.
        apply_mapping (str): Whether to apply column mappings ('Y' for yes, 'N' for no).
        data_path (str): The path to the data folder. Defaults to 'data'.

    Returns:
        str: The path to the generated parquet file.
    """

    folder_parquet = get_folder_path("parquet", data_path)
    os.makedirs(folder_parquet, exist_ok=True)
    parquet_file = f"{folder_parquet}/{period}.parquet"

    folder = get_folder_path(period, data_path)
    full_paths = get_full_paths(folder)
    dfs = get_concat_dfs(full_paths)

    dfs = remove_duplicates(dfs, dup_strategy)

    if apply_mapping == "Y":
        dfs = apply_mappings(dfs, mappings)

    dfs.to_parquet(parquet_file, engine="pyarrow")

    print(
        f"Parsed and created parquet file for {period} with {dfs.shape[0]} rows and {dfs.shape[1]} columns."
    )

    return parquet_file


def remove_duplicates(df: str, strategy: str) -> pd.DataFrame:
    """
    Removes duplicates from a dataframe based on the 'link', 'id' or 'title' column.
    If there are duplicates, the most recent entry is kept.

    Args:
    df (DataFrame): A pandas DataFrame.
    strategy (str): The strategy to use for removing duplicates. Allowed values are 'id', 'link', 'title' or 'None'.

    Returns:
    DataFrame: A pandas DataFrame with duplicates removed with selected strategy.
    """

    if strategy == "None":
        return df

    strategies = ["id", "link", "title"]

    if strategy not in strategies:
        raise ValueError(
            f"Invalid strategy: {strategy}. Allowed strategies are {strategies}"
        )

    no_dups_df = df.copy()
    no_dups_df["updated"] = pd.to_datetime(no_dups_df["updated"])

    no_dups_df = no_dups_df.sort_values(
        by=[strategy, "updated"], ascending=[True, False]
    )

    no_dups_df = no_dups_df.drop_duplicates(subset=[strategy], keep="first")

    no_dups_df = no_dups_df.sort_values(by="updated", ascending=False).reset_index(
        drop=True
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

    # Select only the columns that correspond to the values in the mappings dictionary
    selected_columns = {k: v for k, v in mappings.items() if k in df.columns}
    mapped_df = df[selected_columns.keys()].copy()

    # Rename the columns to the corresponding keys in the mappings dictionary
    mapped_df.rename(columns=selected_columns, inplace=True)

    return mapped_df


def delete_files(period: str, data_path: str = "data"):
    """
    Deletes the folder with the given period name inside the data folder.

    Args:
    period (str): The name of the period whose folder is to be deleted.
    data_path (str): The path to the data folder. Defaults to 'data'.

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
