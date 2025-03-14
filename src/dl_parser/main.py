from src.dl_parser.utils import (
    get_source_data,
)


def main():
    """
    Main function to run the parser from the command line.

    This function retrieves the source data, downloads and extracts the zip files for each period,
    processes the XML files, concatenates the data into a DataFrame, removes duplicates, and saves
    the final DataFrame as a parquet file.

    Usage:
        python main.py
    """
    source_data = get_source_data()
    print(
        "Welcome to the Spanish Public Procurement Database (SPPD) downloader and parser."
    )
    years = [key for key in source_data.keys() if len(key) == 4]
    months = [key for key in source_data.keys() if len(key) > 4]

    period = input(
        f"""
                   Type one of the available periods of data:
                   Full years: {', '.join(years)}
                   Months of current year: {', '.join(months)}
                   """
    )

    if period not in years + months:
        print("Sorry, incorrect input.")
    else:
        print(f"You chose {period}, download and parsing will start shortly.")


if __name__ == "__main__":
    main()
