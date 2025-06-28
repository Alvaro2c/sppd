from src.db_init.utils import (
    get_parquet_base_table,
    create_db_local_folder,
    get_db_codice_tables,
    create_duckdb_db,
)
from unittest.mock import patch, MagicMock
import polars as pl
import os


def test_get_parquet_base_table(tmp_path, sample_df_with_duplicates):
    df1 = sample_df_with_duplicates
    df2 = sample_df_with_duplicates

    # Create sample parquet files
    df1.write_parquet(f"{tmp_path}/file1.parquet")
    df2.write_parquet(f"{tmp_path}/file2.parquet")

    with patch("src.common.utils.get_full_paths") as mock_get_full_paths:
        mock_get_full_paths.return_value = [
            f"{tmp_path}/file1.parquet",
            f"{tmp_path}/file2.parquet",
        ]

    output_file = get_parquet_base_table(parquet_path=tmp_path, local_db_path=tmp_path)

    # Verify the concatenated file
    result_df = pl.read_parquet(output_file)
    expected_df = df1.slice(1, 1)
    expected_df = expected_df.with_columns(
        pl.col("updated").str.strptime(pl.Datetime, strict=False)
    )
    assert len(result_df) == len(expected_df)
    assert list(result_df.columns) == list(expected_df.columns)
    assert result_df.equals(expected_df)


def test_create_db_local_folder(tmp_path):
    """Test create_db_local_folder function"""
    # Test creating a new folder
    test_folder = "test_local_db"
    folder_path = os.path.join(tmp_path, "data", test_folder)

    with patch("os.path.join") as mock_join, patch(
        "os.path.exists"
    ) as mock_exists, patch("os.makedirs") as mock_makedirs:

        mock_join.return_value = folder_path
        mock_exists.return_value = False

        create_db_local_folder(test_folder)

        mock_makedirs.assert_called_once_with(folder_path)


def test_get_db_codice_tables(sample_codice_data, tmp_path):
    """Test get_db_codice_tables function"""
    # Create sample DataFrame for codice
    sample_df = pl.DataFrame(
        {
            "code": ["OBJ", "SUBJ"],
            "nombre": ["Automatically evaluated", "Not automatically evaluated"],
        }
    )

    with patch("src.db_init.utils.get_latest_codices") as mock_get_latest, patch(
        "src.db_init.utils.get_codice_df"
    ) as mock_get_df:

        mock_get_latest.return_value = sample_codice_data
        mock_get_df.return_value = sample_df

        result = get_db_codice_tables(
            codice_url="https://example.com",
            data_path=str(tmp_path),
            with_mappings=True,
            mapping="ot",
        )

        # Verify the result
        assert isinstance(result, list)
        assert len(result) == 3  # Should have 3 codices for "ot" mapping

        # Verify files were created
        for path in result:
            assert os.path.basename(path) in [
                "ContractCode.parquet",
                "CPV2008.parquet",
                "TenderingProcessCode.parquet",
            ]


def test_create_duckdb_db(tmp_path, sample_parquet_files_data):
    """Test create_duckdb_db function"""
    # Create sample parquet files in local_db directory
    local_db_path = os.path.join(tmp_path, "data", "local_db")
    os.makedirs(local_db_path, exist_ok=True)

    # Create sample parquet files
    sample_parquet_files_data.write_parquet(
        os.path.join(local_db_path, "base_table.parquet")
    )
    sample_parquet_files_data.write_parquet(
        os.path.join(local_db_path, "ContractCode.parquet")
    )

    with patch("glob.glob") as mock_glob, patch(
        "duckdb.connect"
    ) as mock_connect, patch("os.path.exists") as mock_exists, patch(
        "os.remove"
    ) as mock_remove:

        # Mock glob to return our parquet files
        mock_glob.return_value = [
            os.path.join(local_db_path, "base_table.parquet"),
            os.path.join(local_db_path, "ContractCode.parquet"),
        ]

        # Mock database existence
        mock_exists.return_value = True

        # Mock duckdb connection
        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        create_duckdb_db("test_db")

        # Verify database was created
        mock_connect.assert_called_once()
        mock_remove.assert_called_once()  # Should remove existing database

        # Verify tables were created for each parquet file
        assert mock_con.execute.call_count == 2

        # Verify connection was closed
        mock_con.close.assert_called_once()
