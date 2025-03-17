from src.dl_parser.utils import get_source_data, dl_parser


def main():
    """
    Main function to run the parser from the command line.

    This function retrieves the source data, downloads and extracts the zip files for each period,
    processes the XML files, concatenates the data into a DataFrame, removes duplicates, and saves
    the final DataFrame as a parquet file.

    Usage:
        python main.py
    """

    source_url = (
        "https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/"
        "Paginas/LicitacionesContratante.aspx"
    )
    source_data = get_source_data(source_url=source_url)
    periods = source_data.keys()
    dup_strats = ["id", "link", "title", "None"]
    yes_or_no = ["Y", "N"]
    years = [period for period in periods if len(period) == 4]
    months = [period for period in periods if len(period) > 4]

    print(
        """
    Welcome to the Spanish Public Procurement Database (SPPD) downloader and parser.
    You will be able to download ATOM files and parse them into parquet files.
    """
    )

    def prompt_for_period():
        return input(
            f"""
        Type one of the available periods of data
        (More recent years have more data/files and will take longer to download and parse):
        Full years: {', '.join(years)}
        Months of current year: {', '.join(months)}\n
        """
        )

    selected_period = prompt_for_period()

    while selected_period not in periods:
        print(
            "\nSorry, incorrect input. Please type one of the mentioned.\n\n-----------------\n"
        )
        selected_period = prompt_for_period()

    def prompt_for_dup_strat():
        return input(
            f"""
        You chose {selected_period}.
        Please select a duplicate handling strategy (type the strategy in lowercases):
        1. id
        2. link
        3. title
        4. None (if you want to keep duplicates in the raw parquet files)\n
        """
        )

    dup_strat = prompt_for_dup_strat()

    while dup_strat not in dup_strats:
        print(
            "\nSorry, incorrect input. Please type one of the mentioned strategies (id, link, title or None).\n"
        )
        dup_strat = prompt_for_dup_strat()

    def prompt_for_file_handling():
        return input(
            f"""
        You chose {selected_period} with duplicate handling strategy {dup_strat}.
        Do you want to delete the downloaded ATOM files? (Y/N)\n
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
        Period: {selected_period}
        Duplicate handling strategy: {dup_strat}
        Delete downloaded files: {del_files}

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
        parquet_file = dl_parser(
            source_data=source_data,
            selected_period=selected_period,
            dup_strat=dup_strat,
            del_files=del_files,
        )

        print(f"\nFile location: {parquet_file}\n")


if __name__ == "__main__":
    main()
