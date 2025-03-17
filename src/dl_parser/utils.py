# data processing
import pandas as pd

# web/xml scraping
import requests
import re
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# local file handling
import zipfile
import io
import os
from tqdm import tqdm

from src.dl_parser.mappings import mappings


def get_soup(url: str) -> BeautifulSoup:
    """
    Fetches the HTML content from the given URL and returns a BeautifulSoup object.

    Args:
        url (str): The URL of the webpage to fetch.

    Returns:
        BeautifulSoup: A BeautifulSoup object containing the parsed HTML content of the webpage.
    """
    response = requests.get(url)

    return BeautifulSoup(response.text, "html.parser")


def extract_digits_from_url(url):
    """
    Extracts the digits from the URL that correspond to the period of the file to be downloaded.
    """
    match = re.search(r"_(\d{4,6})\.zip", url)
    if match:
        return match.group(1)
    return None


def get_source_data(source_url: str):
    """
    Get the source data from the url and return a dict with the period as key and the link to the data as value.
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


def recursive_find_value(tag: str, dictionary: dict) -> tuple:
    """
    Recursively searches for a given tag (key) in a nested dictionary and returns a tuple containing it and its value.

    Args:
        tag (str): The key to search for in the dictionary.
        dictionary (dict): The dictionary to search within, which may contain nested dictionaries.

    Returns:
        tuple: A tuple containing the tag and its corresponding value if found.
        None: If the tag is not found in the dictionary.
    """
    if tag in dictionary:
        return tag, dictionary[tag]
    else:
        for key, value in dictionary.items():
            if isinstance(value, dict):
                result = recursive_find_value(tag, value)
                if result:
                    return result
                else:
                    pass


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
        file (str): The path to the ATOM file to be parsed.

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


def get_df(entries: list, ns: dict, mappings: dict) -> pd.DataFrame:
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

            # Add specific information to entry data
            for k, v in mappings.items():
                try:
                    entry_data[k] = recursive_find_value(v, flat_details)[1]
                except TypeError:
                    entry_data[k] = None

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


def get_folder_path(period: str, data_path: str):
    """
    This function receives the period for which the data is downloaded.
    It returns the path to the folder where the data is downloaded.
    If the folder does not exist, it creates it.

    Parameters:
    period (str): The period for which the data is downloaded.

    Returns:
    str: The path to the folder where the data is downloaded.
    """

    folder = os.path.join(data_path, period)
    os.makedirs(folder, exist_ok=True)

    return folder


def get_full_paths(folder: str):
    """
    This function receives the folder path where the data is downloaded.
    It returns a list with the full paths of the files in the folder.

    Parameters:
    folder (str): The path to the folder where the data is downloaded.

    Returns:
    list: A list with the full paths of the files in the folder.
    """
    files = os.listdir(folder)
    full_paths = [os.path.join(folder, file) for file in files]
    full_paths.sort()

    return full_paths


def get_concat_dfs(paths: list, mappings: dict) -> pd.DataFrame:
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
            df = get_df(entries, ns, mappings)
            dfs.append(df)
            pbar.update(1)

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def get_full_parquet(period: str, data_path: str = "data"):
    """
    Generates a full parquet file for the given period.
    This function performs the following steps:
    1. Retrieves the folder path for the specified period.
    2. Obtains the full paths of files within the folder.
    3. Concatenates the dataframes from the full paths.
    4. Saves the concatenated dataframe as a parquet file in the specified directory.
    Args:
        period (str): The period for which the parquet file is to be generated.
    Returns:
        parquet_file (str): parquet_file path for duplicate handling strategy
    """

    folder_parquet = get_folder_path("parquet", data_path)
    os.makedirs(folder_parquet, exist_ok=True)
    parquet_file = f"{folder_parquet}/{period}.parquet"

    folder = get_folder_path(period, data_path)
    full_paths = get_full_paths(folder)
    dfs = get_concat_dfs(full_paths, mappings)

    dfs.to_parquet(parquet_file)

    print(
        f"Parsed and created parquet file for {period} with {dfs.shape[0]} rows and {dfs.shape[1]} columns."
    )

    return parquet_file


def remove_duplicates(parquet_path: str, strategy: str) -> pd.DataFrame:
    """
    Removes duplicates from a dataframe based on the 'link', 'id' or 'title' column.
    If there are duplicates, the most recent entry is kept.

    Parameters:
    df (DataFrame): A pandas DataFrame.

    Returns:
    DataFrame: A pandas DataFrame with duplicates removed with selected strategy.
    """

    if strategy == "None":
        return None

    strategies = ["id", "link", "title"]

    if strategy not in strategies:
        raise ValueError(
            f"Invalid strategy: {strategy}. Allowed strategies are {strategies}"
        )

    df = pd.read_parquet(parquet_path)

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


def delete_files(period: str, data_path: str = "data"):
    """
    Deletes the folder with the given period name inside the data folder.

    Parameters:
    period (str): The name of the period whose folder is to be deleted.

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


def dl_parser(source_data: dict, selected_period: str, dup_strat: str, del_files: str):
    """
    Main function to download, process, and save data for a specified period.
    """

    download_and_extract_zip(source_data, selected_period)
    parquet_file = get_full_parquet(selected_period)
    remove_duplicates(parquet_file, dup_strat)
    if del_files == "Y":
        delete_files(selected_period)

    return parquet_file
