from src.dl_parser.utils import (
    get_soup,
    extract_digits_from_url,
    flatten_dict,
    get_source_data,
    recursive_field_dict,
    get_atom_data,
    get_data_list,
    download_and_extract_zip,
    get_folder_path,
    get_full_paths,
    get_concat_df,
    get_full_parquet,
    remove_duplicates,
    apply_mappings,
    delete_files,
    dl_parser,
)
import lxml.etree as ET
from unittest.mock import patch, mock_open, ANY
from bs4 import BeautifulSoup
import polars as pl
import os
from datetime import datetime, timezone


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


def test_get_atom_data(sample_atom_content):
    root = ET.fromstring(sample_atom_content)
    with patch("lxml.etree.parse") as mock_parse, patch(
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


def test_get_data_list(sample_entries, sample_data_list):
    sample_ns = {"cac-place-ext": "http://www.w3.org/2005/Atom"}
    data_list = get_data_list(sample_entries, sample_ns)

    assert data_list == sample_data_list


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


def test_get_concat_df(sample_data_list, sample_df, tmp_path):
    sample_paths = ["data/raw/atom/202101/file1.xml", "data/raw/atom/202101/file2.xml"]
    with patch("src.dl_parser.utils.get_atom_data") as mock_get_atom_data, patch(
        "src.dl_parser.utils.get_data_list"
    ) as mock_get_data_list:
        mock_get_atom_data.return_value = ([], {})
        mock_get_data_list.return_value = sample_data_list

        df = get_concat_df(sample_paths, tmp_path)
        expected_df = pl.concat([sample_df, sample_df], how="diagonal")

        assert len(df) == len(expected_df)
        assert list(df.columns) == list(expected_df.columns)
        assert df.equals(expected_df)


def test_get_full_parquet(sample_period, sample_df, tmp_path):
    with patch("src.dl_parser.utils.get_folder_path") as mock_get_folder_path, patch(
        "src.dl_parser.utils.get_full_paths"
    ) as mock_get_full_paths, patch(
        "src.dl_parser.utils.get_concat_df"
    ) as mock_get_concat_df:
        mock_get_folder_path.side_effect = lambda x, y: os.path.join(y, x)
        mock_get_full_paths.return_value = ["file1.xml", "file2.xml"]
        mock_get_concat_df.return_value = sample_df

        parquet_file = get_full_parquet(
            period=sample_period,
            dup_strategy="None",
            apply_mapping=False,
            raw_data_path=tmp_path,
        )
        expected_parquet_file = f"{tmp_path}/parquet/202101.parquet"

        assert parquet_file == expected_parquet_file


def test_remove_duplicates(sample_df_with_duplicates):
    no_dups_df = remove_duplicates(df=sample_df_with_duplicates, strategy="link")
    expected_df = pl.DataFrame(
        [
            {
                "id": "1",
                "link": "http://example.com",
                "title": "Example Entry",
                "updated": datetime(2023, 1, 2, tzinfo=timezone.utc),
            }
        ]
    )

    assert no_dups_df.equals(expected_df)


def test_delete_files(sample_period, tmp_path):
    with patch("os.path.exists") as mock_exists, patch(
        "os.listdir"
    ) as mock_listdir, patch("os.rmdir") as mock_rmdir:
        mock_exists.return_value = True
        mock_listdir.return_value = ["file1.xml", "file2.xml"]
        delete_files(sample_period, tmp_path)
        mock_rmdir.assert_called_with(f"{tmp_path}/{sample_period}")


def test_dl_parser(sample_source_data, sample_period, sample_parquet_path):

    with patch(
        "src.dl_parser.utils.download_and_extract_zip"
    ) as mock_download_and_extract_zip, patch(
        "src.dl_parser.utils.get_full_parquet"
    ) as mock_get_full_parquet, patch(
        "src.dl_parser.utils.delete_files"
    ) as mock_delete_files:
        mock_get_full_parquet.return_value = sample_parquet_path

        result = dl_parser(
            sample_source_data,
            [sample_period],
            dup_strategy="None",
            apply_mapping="N",
            del_files="Y",
        )

        mock_download_and_extract_zip.assert_called_once_with(
            sample_source_data, sample_period
        )
        mock_get_full_parquet.assert_called_once_with(
            period=sample_period, dup_strategy="None", apply_mapping="N"
        )
        mock_delete_files.assert_called_once_with(sample_period)
        assert result == [sample_parquet_path]


def test_apply_mappings(sample_df_for_mapping, sample_mapping_dict):
    result_df = apply_mappings(sample_df_for_mapping, sample_mapping_dict)

    assert set(["title", "link", "date"]).issubset(result_df.columns)
    assert result_df.item(0, "title") == "Example Title"
    assert result_df.item(0, "link") == "http://example.com"
    assert result_df.item(0, "date") == "2023-01-01"
    assert "extra_field" not in result_df.columns
