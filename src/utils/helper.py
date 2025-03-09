# data processing
import pandas as pd

# web/xml scraping
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# local file handling
import zipfile
import io
import os


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


def get_source_data():
    """
    Get the source data from the url and return a dict with the period as key and the link to the data as value.
    """
    source_url = "https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx"
    soup = get_soup(source_url)

    links = [
        a.get("href")
        for a in soup.find_all("a")
        if "contratacion" in a.get("href") and a.get("href").endswith("zip")
    ]
    periods = [link[113:-4] for link in links]
    source_data = {p: l for p, l in zip(periods, links)}

    return source_data


def recursive_field_dict(field, field_dict: dict):
    """
    Recursively converts an XML element and its children into a nested dictionary.

    Args:
        field (xml.etree.ElementTree.Element): The XML element to be converted.
        field_dict (dict): The dictionary to store the converted XML structure.

    Returns:
        None: The function modifies the field_dict in place to replicate the XML tree structure.
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
    Parses an XML file and retrieves namespace information and entries.

    Args:
        file (str): The path to the XML file to be parsed.

    Returns:
        tuple: A tuple containing:
            - entries (list): A list of XML elements found under the 'entry' tag in the default namespace.
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
    Extracts the main information from the entries of the XML file and returns a DataFrame with the data.

    Parameters:
    entries (list): List of XML elements representing the entries.
    ns (dict): Dictionary of namespaces used in the XML file.
    mapping (dict): Dictionary mapping the desired DataFrame column names to the corresponding XML tags.

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
        recursive_field_dict(details, details_dict)
        flat_details = flatten_dict(details_dict)

        # Add specific information to entry data
        for k, v in mappings.items():
            try:
                entry_data[k] = recursive_find_value(v, flat_details)[1]
            except TypeError:
                entry_data[k] = None

        entry_data.pop("summary")
        entry_data.pop("ContractFolderStatus")

        data.append(entry_data)

    df = pd.DataFrame(data)

    return df


def download_and_extract_zip(source_data: dict, period: str):
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
        folder = os.path.join(os.path.dirname(os.path.abspath("")), "data", period)
        response = requests.get(zip_url)
        with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
            thezip.extractall(folder)


def get_folder_path(period: str):
    """
    This function receives the period for which the data is downloaded.
    It returns the path to the folder where the data is downloaded.

    Parameters:
    period (str): The period for which the data is downloaded.

    Returns:
    str: The path to the folder where the data is downloaded.
    """
    folder = os.path.join(os.path.dirname(os.path.abspath("")), "data", period)

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
    for path in paths:
        entries, ns = get_atom_data(path)
        df = get_df(entries, ns, mappings)
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicates from a dataframe based on the 'id' and 'title' columns.
    If there are duplicates, the most recent entry is kept.

    Parameters:
    df (DataFrame): A pandas DataFrame.

    Returns:
    DataFrame: A pandas DataFrame with dupliactes removed.

    """

    no_dups_df = df.copy()
    no_dups_df["updated"] = pd.to_datetime(no_dups_df["updated"])

    no_dups_df = no_dups_df.sort_values(
        by=["id", "title", "updated"], ascending=[True, True, False]
    )

    no_dups_df = no_dups_df.drop_duplicates(subset=["id", "title"], keep="first")

    no_dups_df.reset_index(drop=True, inplace=True)

    return no_dups_df
