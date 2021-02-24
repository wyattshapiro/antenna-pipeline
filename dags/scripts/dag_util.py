import pandas as pd
import logging


def assert_df_column_has_min_value_distribution(input_file_path, group_column, min_frequency):
    """Check if dataframe column contains reasonable distribution of values.

    Args:
        input_file_path (str): file path, where file contains rows that represent a subscription with signal type
        group_column (str): the name of the column to check
        min_frequency (float): the minimum occurence expected for each group, relative to the size of the batch dataset

    Raises:
        AssertionError: Raise error if there is an unexpected distribution of values
    """
    # validate min occurence
    if min_frequency < 0 or min_frequency > 1:
        error_message = f'min_frequency should be a float between 0 and 1.'
        raise ValueError(error_message)
    
    # read in files
    input_df = pd.read_csv(input_file_path)
    total_rows = len(input_df)
    logging.info('Number of rows: {}'.format(total_rows))

    # get distribution of values
    distribution_by_group = input_df[group_column].value_counts(normalize=True)
    
    # determine if any groups have frequency below minimum threshold
    has_rare_group = distribution_by_group.where(distribution_by_group < min_frequency).any()
    
    # if rare group is detected, then throw error
    error_message = f'Expected at least {min_frequency} frequency for each group in column {group_column}.'
    assert (not has_rare_group), error_message


def assert_df_column_has_no_duplicate_values(input_file_path, unique_column):
    """Check if dataframe has no duplicate rows

    Args:
        input_file_path (str): file path, where file contains rows
        unique_column (str): name of column that should contain unique values

    Raises:
        AssertionException: Raise error if there are repeated item ids
    """
    input_df = pd.read_csv(input_file_path)
    num_rows = len(input_df)
    num_unique_rows = len(input_df[unique_column].unique())

    error_message = f'Expected unique values in {unique_column} column,\
        recieved {num_rows} rows and {num_unique_rows} values in {unique_column} column.'
    assert (num_rows == num_unique_rows), error_message
