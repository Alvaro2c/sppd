# data processing
import pandas as pd

# web/xml scraping
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup


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
            - ns (dict): A dictionary of namespaces with their prefixes.
            - atom (str): The default namespace URI enclosed in curly braces.
            - entries (list): A list of XML elements found under the 'entry' tag in the default namespace.
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

    return ns, atom, entries


def get_main_df(entries: list, ns: dict, mapping: dict) -> pd.DataFrame:
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
        for k, v in mapping.items():
            try:
                entry_data[k] = recursive_find_value(v, flat_details)[1]
            except TypeError:
                entry_data[k] = None

        entry_data.pop("summary")
        entry_data.pop("ContractFolderStatus")

        data.append(entry_data)

    df = pd.DataFrame(data)

    return df
