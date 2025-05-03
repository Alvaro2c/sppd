from src.db_init.utils import concat_parquet_files
from unittest.mock import patch
import polars as pl


def test_concat_parquet_files(tmp_path, sample_df_with_duplicates):
    df1 = sample_df_with_duplicates
    df2 = sample_df_with_duplicates

    # Get first column name for sorting
    sort_column = df1.columns[0]

    # Create sample parquet files
    df1.write_parquet(f"{tmp_path}/file1.parquet")
    df2.write_parquet(f"{tmp_path}/file2.parquet")

    output_file = f"{tmp_path}/output.parquet"

    with patch("src.common.utils.get_full_paths") as mock_get_full_paths:
        mock_get_full_paths.return_value = [
            f"{tmp_path}/file1.parquet",
            f"{tmp_path}/file2.parquet",
        ]

        concat_parquet_files(tmp_path, output_file)

        # Verify the concatenated file
        result_df = pl.read_parquet(output_file).sort(by=sort_column).select(pl.all())
        expected_df = pl.concat([df1, df2]).sort(by=sort_column).select(pl.all())
        assert len(result_df) == len(expected_df)
        assert list(result_df.columns) == list(expected_df.columns)
        assert result_df.equals(expected_df)
