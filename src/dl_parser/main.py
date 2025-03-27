from src.dl_parser.utils import get_source_data, dl_parser


def main():
    """
    Main function to run the parser from the command line.

    This function retrieves the source data, downloads and extracts the zip files for each period,
    processes the XML files, concatenates the data into a DataFrame and saves the final DataFrame
    as a parquet file.

    Usage:
        python main.py
    """

    source_url = (
        "https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/"
        "Paginas/LicitacionesContratante.aspx"
    )
    source_data = get_source_data(source_url=source_url)
    periods = source_data.keys()
    yes_or_no = ["Y", "N"]
    years = [period for period in periods if len(period) == 4]
    months = [period for period in periods if len(period) > 4]

    print(
        """
    Welcome to the Spanish Public Procurement Database (SPPD) downloader and parser.
    You will be able to download ATOM files per period and parse them into parquet files.
    """
    )

    def prompt_for_period():
        return (
            input(
                f"""
        Type one or more of the available periods of data, separated by commas
        (More recent years have more data/files and will take longer to download and parse):
        Full years: {', '.join(years)}
        Months of current year: {', '.join(months)}\n
        """
            )
            .replace(" ", "")
            .split(",")
        )

    selected_periods = prompt_for_period()

    for selected_period in selected_periods:
        if selected_period not in periods:
            print(
                "\nSorry, incorrect input. Please type one of the mentioned.\n\n-----------------\n"
            )
            selected_periods = prompt_for_period()

    def prompt_for_file_handling():
        return input(
            f"""
        You chose {selected_periods}.
        Do you want to delete the downloaded ATOM files after parsing? (Y/N)\n
        """
        )

    del_files = prompt_for_file_handling()

    while del_files not in yes_or_no:
        print("\nSorry, incorrect input. Please select yes (Y) or no (N).\n")
        del_files = prompt_for_file_handling()

    def prompt_for_confirmation():
        return input(
            f"""
        You chose the following options:
        Period(s): {selected_periods}
        Delete downloaded files: {del_files}

        Each period will be downloaded and parsed into a parquet file.

        Confirm your selections? (Y/N)\n
        """
        )

    confirmation = prompt_for_confirmation()

    while confirmation not in yes_or_no:
        print("\nSorry, incorrect input. Please select yes (Y) or no (N).\n")
        confirmation = prompt_for_confirmation()

    if confirmation == "N":
        print("\nRestarting the process...\n")
        main()
    else:
        parquet_files = dl_parser(
            source_data=source_data,
            selected_periods=selected_periods,
            del_files=del_files,
        )

        print("\nFile name:")
        for parquet_file in parquet_files:
            print(parquet_file)
        print("\nProcess completed successfully!")


if __name__ == "__main__":
    main()
