from src.common.utils import get_soup
from src.dl_parser.utils import remove_duplicates, apply_mappings

import os
import polars as pl
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def concat_parquet_files(folder_path: str, output_file: str) -> str:
    """
    Concatenates multiple parquet files from a specified folder into a single parquet file using polars.

    Args:
        folder_path (str): The path to the folder containing the parquet files to concatenate.
        output_file (str): The path where the concatenated parquet file will be saved.

    Returns:
        str: The path to the concatenated parquet file.
    """
    parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
    df = pl.concat([pl.read_parquet(f) for f in parquet_files], how="diagonal")
    output_path = os.path.join(folder_path, output_file)
    df.write_parquet(output_path)
    print(f"Concatenated parquet file saved as '{output_path}'")

    return output_path


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


def get_codice_df(codice_direct_link: str) -> pl.DataFrame:
    """
    Get the codice dataframe from the direct link to the codice.

    Args:
        codice_direct_link (str): Direct link to the codice.
    Returns:
        codice_df (pl.DataFrame): DataFrame with the codice information.
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
    return pl.DataFrame(codice_data)


def create_db_local_folder(local_folder: str = "local_db") -> None:
    """
    Create a local folder for the database if it doesn't exist.

    Parameters:
    local_folder (str): Path to the folder to be created.

    Returns:
    local_folder_path (str): Path to the created folder.
    """
    try:
        # Create the folder if it doesn't exist
        folder_path = os.path.join("data", local_folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"Folder {local_folder} created.")
        else:
            print(f"Folder {local_folder} already exists.")
    except Exception as e:
        raise RuntimeError(f"Error creating folder {local_folder}: {e}")


def get_db_base_table(
    apply_mapping: str,
    dup_strategy: str,
    parquet_path: str = "data/raw/parquet",
    local_db_path: str = "data/local_db",
) -> str:
    """
    Generate a base table DataFrame from parquet files.

    Parameters:
    apply_mappings (str): Flag to determine if mappings should be applied ("Y" for yes, otherwise no).
    dup_strategy (str): Strategy to handle duplicate records.
    parquet_folder (str): Folder containing parquet files. Default is "parquet".

    Returns:
    str: Path to the processed DataFrame saved as a parquet file.
    """
    try:
        # Get the full parquet db
        raw_base_table_path = concat_parquet_files(
            parquet_path, "raw_base_table.parquet"
        )

        # Remove duplicates
        base_table_proc = remove_duplicates(
            pl.read_parquet(raw_base_table_path), dup_strategy
        )

        # Apply mappings if required
        if apply_mapping.upper() == "Y":
            base_table_proc = apply_mappings(base_table_proc)

        # Save the processed DataFrame to a parquet file
        base_table_path = f"{local_db_path}/base_table.parquet"
        base_table_proc.write_parquet(base_table_path)

        return base_table_path

    except Exception as e:
        raise RuntimeError(f"Error processing base table: {e}")


def get_db_codice_tables(codice_url: str) -> list:

    latest_codices = get_latest_codices(codice_url)
    codice_dfs = {
        name: (get_codice_df(url), version)
        for name, (url, version) in latest_codices.items()
    }

    codice_paths = []

    # Iterate through the codices and save them to parquet files
    for (codice_name, codice_version), codice_df in codice_dfs.items():
        # Save the DataFrame to a parquet file
        codice_path = f"local_db/{codice_name}_{codice_version}.parquet"
        codice_df.write_parquet(codice_path)
        print(f"Saved {codice_name} version {codice_version} to {codice_path}")
        # Append the paths to the list
        codice_paths.append(codice_path)

    # Return the list of paths to the parquet files
    return codice_paths
