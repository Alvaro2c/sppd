from src.common.utils import get_full_paths, get_soup
import pandas as pd


def concat_parquet_files(folder_path: str, output_file: str) -> None:
    """
    Concatenates multiple parquet files from a specified folder into a single parquet file.

    Args:
        folder_path (str): The path to the folder containing the parquet files to concatenate.
        output_file (str): The path where the concatenated parquet file will be saved.

    Returns:
        None: The function saves the concatenated parquet file to the specified output path.
    """
    parquet_files = get_full_paths(folder_path)
    concatenated_df = pd.concat([pd.read_parquet(file) for file in parquet_files])
    concatenated_df.to_parquet(output_file)
    print(f"Concatenated parquet file saved as '{output_file}'")

    return output_file


def get_latest_codices(codice_url: str) -> dict:
    """
    Get the latest version of all codices from the given URL.
    Currently: "https://contrataciondelestado.es/codice/cl/"

    Args:
        codice_url (str): URL to the codice website.

    Returns:
        Dictionary with the latest version of each codice.
    """

    codice_soup = get_soup(codice_url)

    # Get all codice versions directories
    codice_versions = [
        codice_url + a.text
        for a in codice_soup.find_all("a")
        if not any(char.isalpha() for char in a.text)
    ]

    # Get all codices, regardless of version
    all_codices = []

    for version in codice_versions:
        version_soup = get_soup(version)
        codices = [
            version + a["href"]
            for a in version_soup.find_all("a", href=True)
            if a["href"].endswith(".gc")
        ]
        all_codices.extend(codices)

    # Parse codice URL to get base name and version
    def parse_codice_url(url):
        filename = url.split("/")[-1]
        base_name = (
            filename.split("-")[0] if "-" in filename else filename.split(".gc")[0]
        )
        version = filename.split("-")[-1].split(".gc")[0] if "-" in filename else "1"
        return base_name, version

    # Process all codices and keep only latest versions
    latest_codices = {}

    for codice in all_codices:
        base_name, version = parse_codice_url(codice)
        latest_codices[base_name] = (codice, version)

    return latest_codices


def get_codice_df(codice_direct_link: str) -> pd.DataFrame:
    """
    Get the codice dataframe from the direct link to the codice.

    Args:
        codice_direct_link (str): Direct link to the codice.
    Returns:
        codice_df (pd.DataFrame): DataFrame with the codice information.
    """
    codice_soup = get_soup(codice_direct_link)

    # Get the rows as lists from the codice.
    # Each row value is a tuple with the column reference and the value itself.
    rows = [
        [(value["columnref"], value.text.strip()) for value in row.find_all("value")]
        for row in codice_soup.find_all("row")
    ]

    # Convert to DataFrame
    codice_data = [dict(row) for row in rows]
    return pd.DataFrame(codice_data)
