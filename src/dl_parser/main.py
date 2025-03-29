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

    while not all(selected_period in periods for selected_period in selected_periods):
        print(
            "\nSorry, some of the entered periods are incorrect. Please select correct one(s).\n\n-----------------\n"
        )
        selected_periods = prompt_for_period()

    def prompt_for_duplicate_strategy():
        return input(
            f"""
        You chose {selected_periods}.

        Type a duplicate removal strategy (type 'None' to keep the full raw data).
        1. id
        2. title
        3. link (recommended if selecting a duplicate removal strategy)
        4. None\n
        """
        )

    dup_strategy = prompt_for_duplicate_strategy()

    while dup_strategy not in ["id", "title", "link", "None"]:
        print(
            "\nSorry, incorrect input. Please select one of the mentioned strategies.\n"
        )
        dup_strategy = prompt_for_duplicate_strategy()

    def prompt_for_mapping_application():
        return input(
            f"""
        You chose {selected_periods} and the following duplicate removal strategy: {dup_strategy}.

        Do you want to apply a mapping to the data (select main dimensions only)?
        Select N to keep the full raw data.
        (Y/N)\n
        """
        )

    apply_mapping = prompt_for_mapping_application()

    while apply_mapping not in yes_or_no:
        print("\nSorry, incorrect input. Please select yes (Y) or no (N).\n")
        apply_mapping = prompt_for_mapping_application()

    def prompt_for_file_handling():
        return input(
            f"""
        You chose {selected_periods}, the following duplicate removal strategy: {dup_strategy},
        and the following mapping application: {apply_mapping}.

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
        Duplicate removal strategy: {dup_strategy}
        Apply mapping: {apply_mapping}

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
            dup_strategy=dup_strategy,
            apply_mapping=apply_mapping,
            del_files=del_files,
        )

        print("\nFile name:")
        for parquet_file in parquet_files:
            print(parquet_file)
        print("\nProcess completed successfully!")


if __name__ == "__main__":
    main()
