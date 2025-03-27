import requests
import warnings
import os
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning


def get_soup(url: str) -> BeautifulSoup:
    """
    Fetches the HTML content from the given URL and returns a BeautifulSoup object.

    Args:
        url (str): The URL of the webpage to fetch.

    Returns:
        BeautifulSoup: A BeautifulSoup object containing the parsed HTML content of the webpage.
    """
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    response = requests.get(url)

    return BeautifulSoup(response.text, "html.parser")


def get_folder_path(period: str, data_path: str = "data"):
    """
    This function receives the period for the data you are manipulating.
    It returns the path to the folder where the data is or should be.
    If the folder does not exist, it creates it.

    Parameters:
    period (str): The period for which the data is downloaded.
    data_path(str): The path to the data folder.

    Returns:
    str: The path to the folder where the data is downloaded.
    """

    folder = os.path.join(data_path, period)
    os.makedirs(folder, exist_ok=True)

    return folder


def get_full_paths(folder: str):
    """
    This function receives the folder path where the data is.
    It returns a list with the full paths of the files in the folder.

    Parameters:
    folder (str): The path to the folder where the data is.

    Returns:
    list: A list with the full paths of the files in the folder.
    """
    files = os.listdir(folder)
    full_paths = [os.path.join(folder, file) for file in files]
    full_paths.sort()

    return full_paths
