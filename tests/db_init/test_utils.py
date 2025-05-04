from src.db_init.utils import get_db_base_table
from unittest.mock import patch
import polars as pl


def test_get_db_base_table(tmp_path, sample_df_with_duplicates):
    df1 = sample_df_with_duplicates
    df2 = sample_df_with_duplicates

    # Get first column name for sorting
    sort_column = df1.columns[0]

    # Create sample parquet files
    df1.write_parquet(f"{tmp_path}/file1.parquet")
    df2.write_parquet(f"{tmp_path}/file2.parquet")

    with patch("src.common.utils.get_full_paths") as mock_get_full_paths:
        mock_get_full_paths.return_value = [
            f"{tmp_path}/file1.parquet",
            f"{tmp_path}/file2.parquet",
        ]

    output_file = get_db_base_table(parquet_path=tmp_path, local_db_path=tmp_path)

    # Verify the concatenated file
    result_df = pl.read_parquet(output_file)
    expected_df = df1.slice(1, 1)
    expected_df = expected_df.with_columns(
        pl.col("updated").str.strptime(pl.Datetime, strict=False)
    )
    assert len(result_df) == len(expected_df)
    assert list(result_df.columns) == list(expected_df.columns)
    assert result_df.equals(expected_df)
