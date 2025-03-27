from src.db_init.utils import concat_parquet_files
from unittest.mock import patch
import pandas as pd


def test_concat_parquet_files(tmp_path, sample_df_with_duplicates):
    df1 = sample_df_with_duplicates.copy()
    df2 = sample_df_with_duplicates.copy()

    # Create sample parquet files
    df1.to_parquet(f"{tmp_path}/file1.parquet")
    df2.to_parquet(f"{tmp_path}/file2.parquet")

    output_file = f"{tmp_path}/output.parquet"

    with patch("src.common.utils.get_full_paths") as mock_get_full_paths:
        mock_get_full_paths.return_value = [
            f"{tmp_path}/file1.parquet",
            f"{tmp_path}/file2.parquet",
        ]

        concat_parquet_files(tmp_path, output_file)

        # Verify the concatenated file
        result_df = pd.read_parquet(output_file)
        expected_df = pd.concat([df1, df2], ignore_index=True)
        pd.testing.assert_frame_equal(result_df, expected_df)
