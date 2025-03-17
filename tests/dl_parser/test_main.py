from unittest.mock import patch
from src.dl_parser.main import main


@patch("src.dl_parser.main.get_source_data")
@patch("src.dl_parser.main.dl_parser")
def test_main_happy_path(
    mock_dl_parser, mock_get_source, monkeypatch, mock_source_data
):
    mock_get_source.return_value = mock_source_data
    mock_dl_parser.return_value = "/path/to/output.parquet"

    # Create a list to store our inputs
    responses = ["2023", "id", "Y", "Y"]
    input_iterator = iter(responses)
    monkeypatch.setattr("builtins.input", lambda _: next(input_iterator))

    # Execute main
    main()

    mock_dl_parser.assert_called_once_with(
        source_data=mock_source_data,
        selected_period="2023",
        dup_strat="id",
        del_files="Y",
    )


@patch("src.dl_parser.main.get_source_data")
@patch("src.dl_parser.main.dl_parser")
def test_main_with_invalid_inputs(
    mock_dl_parser, mock_get_source, monkeypatch, mock_source_data
):
    mock_get_source.return_value = mock_source_data
    mock_dl_parser.return_value = "/path/to/output.parquet"

    responses = ["invalid_period", "2023", "invalid_strat", "id", "X", "Y", "Y"]
    input_iterator = iter(responses)
    monkeypatch.setattr("builtins.input", lambda _: next(input_iterator))

    main()

    mock_dl_parser.assert_called_once_with(
        source_data=mock_source_data,
        selected_period="2023",
        dup_strat="id",
        del_files="Y",
    )


@patch("src.dl_parser.main.get_source_data")
@patch("src.dl_parser.main.dl_parser")
def test_main_restart_flow(
    mock_dl_parser, mock_get_source, monkeypatch, mock_source_data
):
    mock_get_source.return_value = mock_source_data
    mock_dl_parser.return_value = "/path/to/output.parquet"

    responses = ["2023", "id", "Y", "N", "2022", "link", "N", "Y"]
    input_iterator = iter(responses)
    monkeypatch.setattr("builtins.input", lambda _: next(input_iterator))

    main()

    mock_dl_parser.assert_called_once_with(
        source_data=mock_source_data,
        selected_period="2022",
        dup_strat="link",
        del_files="N",
    )
