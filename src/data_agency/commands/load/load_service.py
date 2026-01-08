"""
Data loading service for the data agency.

The main workflow:
1. Validates metadata DataFrames for required columns and allowed frequencies
2. Groups metadata by frequency, bilateral flag, and source file
3. Loads each source file and merges columns based on metadata mappings
4. Returns either a single DataFrame or nested dictionaries organized by structure
"""

from typing import Union
import pandas as pd
from lettuce_logger import pp
import numpy as np
from ...common.load_env import METADATA_PATH, DATA_ROOT, raise_if_no_data_root

if METADATA_PATH is not None:
    COUNTRY_CODES_PATH = METADATA_PATH / "country_codes.csv"

REQUIRED_COLUMNS = ["series_code", "source_file", "variable_name", "description", "frequency"]
ALLOWED_FREQUENCIES = ["Q", "M", "A", "D"]


def load_from_metadata(source_file_name: str, meta_df: pd.DataFrame) -> pd.DataFrame:
    """
    Load and transform data from a single CSV file based on metadata specifications.

    Reads the source CSV file, extracts specified columns, renames them according to
    metadata mappings, and attaches metadata as DataFrame attributes. Only keeps
    structural columns and variables specified in the metadata.
    """
    raise_if_no_data_root()

    if meta_df.source_file.nunique() > 1:
        raise ValueError("Metadata DataFrame should only contain one source file in load_from_metadata.")

    full_path = DATA_ROOT / source_file_name  # type: ignore
    try:
        data = pd.read_csv(full_path)
        csv_columns = data.columns.tolist()
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Data file '{full_path}' not found") from e

    if len(data) == 0:
        raise ValueError(f"CSV file '{full_path}' is empty")

    possible_structural_columns = [
        "time",
        "ccode",
        "cgroup",
        "reporter",
        "counterpart",
        "reporter_gr",
        "cpart_gr",
    ]
    series_codes = list(meta_df["series_code"].unique())
    col_names_in_orig_database = [col.split("-")[-1] for col in series_codes]
    orig_col_name_to_var_name = {
        series_code.split("-")[-1]: variable_name
        for series_code, variable_name in zip(meta_df["series_code"], meta_df["variable_name"])
    }

    var_names_to_descriptions = {
        variable_name: description
        for variable_name, description in zip(meta_df["variable_name"], meta_df["description"])
    }

    structural_columns = [col for col in possible_structural_columns if col in data.columns]
    cols_to_keep = structural_columns + col_names_in_orig_database
    cols_to_keep = [col for col in cols_to_keep if col in csv_columns]

    df = data[cols_to_keep].reset_index(drop=True)
    variable_cols = list(set(cols_to_keep) - set(structural_columns))

    if len(variable_cols) < len(col_names_in_orig_database):
        missing_cols = set(col_names_in_orig_database) - set(variable_cols)
        raise ValueError(f"The following columns are not in the data file '{full_path}': {missing_cols}")

    df = df.dropna(subset=variable_cols, how="all")  # type: ignore
    df = df.loc[~(df[variable_cols].isna()).all(axis=1)]
    df.rename(columns=orig_col_name_to_var_name, inplace=True)

    if len(df) == 0:
        raise ValueError(f"No data found in file '{full_path}' for cols :{col_names_in_orig_database}")

    df.attrs = {
        "source_file": meta_df.source_file[0],
        "column_description": var_names_to_descriptions,
        "frequency": meta_df.frequency[0],
        "bilateral": meta_df.bilateral[0],
    }
    return df


def validate_and_merge_metadata_lis(metadata_lis: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Validate metadata DataFrames and merge them into a single DataFrame.

    Checks that all DataFrames have required columns and valid frequency values,
    then concatenates them. This ensures consistency across all metadata inputs
    before processing.
    """

    for df in metadata_lis:
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"All input for load must be pandas DataFrames with columns: {REQUIRED_COLUMNS}")
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                raise ValueError(f"Input DataFrame in load is missing required column: {col}")
        invalid_freqs = df["frequency"].unique()
        invalid_freqs = [freq for freq in invalid_freqs if freq not in ALLOWED_FREQUENCIES]
        if invalid_freqs:
            raise ValueError(f"frequency columns must be {ALLOWED_FREQUENCIES}. Found: {invalid_freqs}")
    df_all = pd.concat(metadata_lis, ignore_index=True)
    if df_all.empty:
        raise ValueError("No data available in the provided metadata DataFrames")
    return df_all


def load(metadata_lis: Union[pd.DataFrame, list[pd.DataFrame]]) -> Union[pd.DataFrame, dict[str, pd.DataFrame]]:
    """
    Load economic data based on metadata specifications and organize by structure.

    This is the main entry point for loading data. It processes metadata to determine
    which files to load, groups data by frequency and bilateral structure, then merges
    data sources appropriately. The return structure depends on how many different
    frequencies and bilateral configurations are present.

    The function groups data by:
    - Frequency (Q/M/A/D) - different time frequencies cannot be merged
    - Bilateral flag - determines merge keys (bilateral uses reporter/counterpart,
      unilateral uses ccode/cgroup)
    - Source file - each source is loaded separately then merged

    Args:
        metadata_lis: Single DataFrame or list of DataFrames containing metadata
                     specifications for data loading

    Returns:
        - Single DataFrame: If only one frequency and bilateral configuration
        - Dict[str, DataFrame]: If one frequency but multiple bilateral configurations
          Keys are boolean bilateral flags
        - Dict[str, DataFrame]: If multiple frequencies, keys combine frequency and
          bilateral info (e.g., "Q_bilateral", "M", "A_bilateral")
    """
    raise_if_no_data_root()
    # return dataframe with attr about columns, source files, frequency, bilateral
    if isinstance(metadata_lis, pd.DataFrame):
        metadata_lis = [metadata_lis]

    # raise error if validation fails
    meta_all = validate_and_merge_metadata_lis(metadata_lis)

    # # add missing columns from series codes if nessesary
    # meta_all = prepare_df_columns(meta_all)

    meta_by_freq_bilateral_source = {}
    for keys, group in meta_all.groupby(["frequency", "source_file", "bilateral"]):
        freq, source, bilateral = keys
        if freq not in meta_by_freq_bilateral_source:
            meta_by_freq_bilateral_source[freq] = {}
        if bilateral not in meta_by_freq_bilateral_source[freq]:
            meta_by_freq_bilateral_source[freq][bilateral] = {}
        meta_by_freq_bilateral_source[freq][bilateral][source] = group.reset_index(drop=True)

    dfs = {}
    merge_keys_bilateral = {
        True: ["time", "reporter", "counterpart", "reporter_gr", "cpart_gr"],
        False: ["time", "ccode", "cgroup"],
    }
    column_descriptions = {
        np.True_: {
            "reporter": "ISO 3166 Alpha-2 code extended with custom codes for aggregated groups",
            "counterpart": "ISO 3166 Alpha-2 code extended with custom codes for aggregated groups",
            "reporter_gr": "reporter group from [AE, Plus3, EMDE, ASEAN5, BCLMV, SmallTerritories,Others]",
            "cpart_gr": "counterpart group from [AE, Plus3, EMDE, ASEAN5, BCLMV, SmallTerritories,Others]",
            "reporter_cname": "Country name of the reporter",
            "cpart_cname": "Country name of the counterpart",
        },
        np.False_: {
            "cname": "Country name",
            "ccode": "ISO 3166 Alpha-2 code extended with custom codes for aggregated groups",
            "cgroup": "Country group from [AE, Plus3, EMDE, ASEAN5, BCLMV, SmallTerritories,Others]",
        },
    }

    country_codes_df = pd.read_csv(COUNTRY_CODES_PATH)
    country_codes_df = country_codes_df[["ccode", "cname"]].copy()
    for freq, meta_by_bilateral_source in meta_by_freq_bilateral_source.items():
        if freq not in dfs:
            dfs[freq] = {}
        for bilateral, meta_by_source in meta_by_bilateral_source.items():
            keys = merge_keys_bilateral[bilateral]
            dfs[freq][bilateral] = pd.DataFrame(columns=keys)
            attrs = {
                "source_files": list(meta_by_source.keys()),
                "frequency": freq,
                "bilateral": bilateral,
                "column_description": column_descriptions[bilateral].copy(),
            }
            for source, meta_df in meta_by_source.items():
                data_df = load_from_metadata(source, meta_df)
                attrs["column_description"] = {**attrs["column_description"], **data_df.attrs["column_description"]}
                dfs[freq][bilateral] = pd.merge(dfs[freq][bilateral], data_df, on=keys, how="outer")

            # Add country names from country_codes_df
            if bilateral:
                dfs[freq][bilateral] = dfs[freq][bilateral].merge(
                    country_codes_df, left_on="reporter", right_on="ccode", how="left"
                )
                dfs[freq][bilateral] = dfs[freq][bilateral].merge(
                    country_codes_df, left_on="counterpart", right_on="ccode", how="left", suffixes=("", "_cpart")
                )
                dfs[freq][bilateral].rename(
                    columns={"cname": "reporter_cname", "cname_cpart": "cpart_cname"}, inplace=True
                )
            else:
                dfs[freq][bilateral] = dfs[freq][bilateral].merge(country_codes_df, on="ccode", how="left")

            dfs[freq][bilateral].attrs = attrs

    if len(dfs) == 1 and len(dfs[next(iter(dfs))]) == 1:
        # If there's only one frequency and one bilateral option, return a single DataFrame
        return next(iter(dfs.values()))[next(iter(dfs[next(iter(dfs))]))]

    if len(dfs) == 1:
        # If there's only one frequency, return a dictionary with bilateral options
        return dfs[next(iter(dfs))]

    else:
        # If there are multiple frequencies, return a dictionary with frequencies as keys
        combined_data = {}
        for freq, bilateral_data in dfs.items():
            for is_bilateral, df in bilateral_data.items():
                key = f"{freq}{'_bilateral' if is_bilateral else ''}"
                combined_data[key] = df
    return combined_data
