from src.dl_parser.utils import (
    get_soup,
    extract_digits_from_url,
    flatten_dict,
    get_source_data,
    recursive_field_dict,
    recursive_find_value,
    get_atom_data,
    get_df,
    download_and_extract_zip,
    get_folder_path,
    get_full_paths,
    get_concat_dfs,
    get_full_parquet,
    remove_duplicates,
    delete_files,
    dl_parser,
    concat_parquet_files,
    get_latest_codices,
)
import xml.etree.ElementTree as ET
from unittest.mock import patch, mock_open, ANY
from bs4 import BeautifulSoup
import pandas as pd
import os


def test_get_soup(sample_url, sample_html_content):
    with patch("requests.get") as mock_get:
        mock_get.return_value.text = sample_html_content
        soup = get_soup(sample_url)
        assert isinstance(soup, BeautifulSoup)


def test_extract_digits_from_url():
    url_with_digits = "https://example.com/file_202101.zip"
    url_without_digits = "https://example.com/file.zip"
    url_with_short_digits = "https://example.com/file_21.zip"

    assert extract_digits_from_url(url_with_digits) == "202101"
    assert extract_digits_from_url(url_without_digits) is None
    assert extract_digits_from_url(url_with_short_digits) is None


def test_flatten_dict(sample_nested_dict):
    flat_dict = flatten_dict(sample_nested_dict)
    expected_dict = {"a.b.c": 1, "d": 2}
    assert flat_dict == expected_dict


def test_get_source_data(sample_url, sample_html_content):
    with patch("requests.get") as mock_get:
        mock_get.return_value.text = sample_html_content
        source_data = get_source_data(sample_url)
        expected_data = {
            "202502": ("https://contrataciondelsectorpublico.gob.es/3_202502.zip"),
            "2020": ("https://contrataciondelsectorpublico.gob.es/3_2020.zip"),
        }
        assert source_data == expected_data


def test_recursive_field_dict():
    sample_xml_content = """
    <root>
        <child1>value1</child1>
        <child2>
            <subchild>value2</subchild>
        </child2>
    </root>
    """
    root = ET.fromstring(sample_xml_content)
    field_dict = {}
    recursive_field_dict(root, field_dict)
    expected_dict = {"child1": "value1", "child2": {"subchild": "value2"}}
    assert field_dict == expected_dict


def test_recursive_find_value(sample_nested_dict):
    tag, value = recursive_find_value("c", sample_nested_dict)
    assert tag == "c"
    assert value == 1


def test_get_atom_data(sample_atom_content):
    root = ET.fromstring(sample_atom_content)
    with patch("xml.etree.ElementTree.parse") as mock_parse, patch(
        "builtins.open", mock_open(read_data=sample_atom_content)
    ):
        mock_parse.return_value = type("obj", (object,), {"getroot": lambda: root})
        entries, ns = get_atom_data("dummy_path.xml")

        assert len(entries) == 1
        assert (
            entries[0].find("{http://www.w3.org/2005/Atom}title").text
            == "Example Entry"
        )
        assert ns[""] == "http://www.w3.org/2005/Atom"


def test_get_df(sample_entries, sample_mappings):
    sample_ns = {"cac-place-ext": "http://www.w3.org/2005/Atom"}
    df = get_df(sample_entries, sample_ns, sample_mappings)
    expected_df = pd.DataFrame(
        [
            {
                "title": "Example Entry",
                "link": "http://example.com",
                "updated": "2023-01-01T00:00:00Z",
            }
        ]
    )
    pd.testing.assert_frame_equal(df, expected_df)


def test_download_and_extract_zip(sample_source_data, sample_period, tmp_path):
    with patch("requests.get") as mock_get, patch("zipfile.ZipFile") as mock_zip, patch(
        "builtins.open", mock_open()
    ):
        mock_get.return_value.content = b"dummy zip content"
        mock_zip.return_value.infolist.return_value = []
        download_and_extract_zip(sample_source_data, sample_period, tmp_path)
        mock_get.assert_called_with("https://example.com/contratacion202101.zip")
        mock_zip.assert_called_with(ANY)


def test_get_folder_path(sample_period, tmp_path):
    with patch("os.makedirs") as mock_makedirs:
        folder_path = get_folder_path(sample_period, tmp_path)
        expected_path = os.path.join(tmp_path, sample_period)
        mock_makedirs.assert_called_with(expected_path, exist_ok=True)
        assert folder_path == expected_path


def test_get_full_paths(sample_folder):
    sample_files = ["file1.xml", "file2.xml"]
    with patch("os.listdir") as mock_listdir:
        mock_listdir.return_value = sample_files
        full_paths = get_full_paths(sample_folder)
        expected_paths = [os.path.join(sample_folder, file) for file in sample_files]
        assert full_paths == expected_paths


def test_get_concat_dfs(sample_mappings):
    sample_paths = ["data/202101/file1.xml", "data/202101/file2.xml"]
    with patch("src.dl_parser.utils.get_atom_data") as mock_get_atom_data, patch(
        "src.dl_parser.utils.get_df"
    ) as mock_get_df:
        mock_get_atom_data.return_value = ([], {})
        mock_get_df.return_value = pd.DataFrame(
            [
                {
                    "title": "Example Entry",
                    "link": "http://example.com",
                    "updated": "2023-01-01T00:00:00Z",
                }
            ]
        )
        df = get_concat_dfs(sample_paths, sample_mappings)
        expected_df = pd.DataFrame(
            [
                {
                    "title": "Example Entry",
                    "link": "http://example.com",
                    "updated": "2023-01-01T00:00:00Z",
                },
                {
                    "title": "Example Entry",
                    "link": "http://example.com",
                    "updated": "2023-01-01T00:00:00Z",
                },
            ]
        )
        pd.testing.assert_frame_equal(df, expected_df)


def test_get_full_parquet(sample_period, tmp_path):
    with patch("src.dl_parser.utils.get_folder_path") as mock_get_folder_path, patch(
        "src.dl_parser.utils.get_full_paths"
    ) as mock_get_full_paths, patch(
        "src.dl_parser.utils.get_concat_dfs"
    ) as mock_get_concat_dfs, patch(
        "pandas.DataFrame.to_parquet"
    ) as mock_to_parquet:
        mock_get_folder_path.side_effect = lambda x, y: os.path.join(y, x)
        mock_get_full_paths.return_value = ["file1.xml", "file2.xml"]
        mock_get_concat_dfs.return_value = pd.DataFrame(
            [
                {
                    "title": "Example Entry",
                    "link": "http://example.com",
                    "updated": "2023-01-01T00:00:00Z",
                }
            ]
        )
        parquet_file = get_full_parquet(sample_period, "None", tmp_path)
        expected_parquet_file = f"{tmp_path}/parquet/202101.parquet"
        mock_to_parquet.assert_called_with(expected_parquet_file)
        assert parquet_file == expected_parquet_file


def test_remove_duplicates(sample_df_with_duplicates):
    with patch("pandas.read_parquet") as mock_read_parquet:
        mock_read_parquet.return_value = sample_df_with_duplicates
        no_dups_df = remove_duplicates(sample_df_with_duplicates, "id")
        expected_df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "link": "http://example.com",
                    "title": "Example Entry",
                    "updated": pd.Timestamp("2023-01-02T00:00:00Z", tz="UTC"),
                }
            ]
        )
        pd.testing.assert_frame_equal(no_dups_df, expected_df)


def test_delete_files(sample_period, tmp_path):
    with patch("os.path.exists") as mock_exists, patch(
        "os.listdir"
    ) as mock_listdir, patch("os.rmdir") as mock_rmdir:
        mock_exists.return_value = True
        mock_listdir.return_value = ["file1.xml", "file2.xml"]
        delete_files(sample_period, tmp_path)
        mock_rmdir.assert_called_with(f"{tmp_path}/{sample_period}")


def test_dl_parser(
    sample_source_data, sample_period, sample_df_with_duplicates, sample_parquet_path
):

    with patch(
        "src.dl_parser.utils.download_and_extract_zip"
    ) as mock_download_and_extract_zip, patch(
        "src.dl_parser.utils.get_full_parquet"
    ) as mock_get_full_parquet, patch(
        "src.dl_parser.utils.remove_duplicates"
    ) as mock_remove_duplicates, patch(
        "src.dl_parser.utils.delete_files"
    ) as mock_delete_files:
        mock_get_full_parquet.return_value = sample_parquet_path
        mock_remove_duplicates.return_value = sample_df_with_duplicates

        result = dl_parser(
            sample_source_data, sample_period, dup_strat="link", del_files="Y"
        )

        mock_download_and_extract_zip.assert_called_once_with(
            sample_source_data, sample_period
        )
        mock_get_full_parquet.assert_called_once_with(sample_period)
        mock_remove_duplicates.assert_called_once_with(sample_parquet_path, "link")
        mock_delete_files.assert_called_once_with(sample_period)
        assert result == sample_parquet_path


def test_concat_parquet_files(tmp_path, sample_df_with_duplicates):
    df1 = sample_df_with_duplicates.copy()
    df2 = sample_df_with_duplicates.copy()

    # Create sample parquet files
    df1.to_parquet(f"{tmp_path}/file1.parquet")
    df2.to_parquet(f"{tmp_path}/file2.parquet")

    output_file = f"{tmp_path}/output.parquet"

    with patch("src.dl_parser.utils.get_full_paths") as mock_get_full_paths:
        mock_get_full_paths.return_value = [
            f"{tmp_path}/file1.parquet",
            f"{tmp_path}/file2.parquet",
        ]

        concat_parquet_files(tmp_path, output_file)

        # Verify the concatenated file
        result_df = pd.read_parquet(output_file)
        expected_df = pd.concat([df1, df2], ignore_index=True)
        pd.testing.assert_frame_equal(result_df, expected_df)
