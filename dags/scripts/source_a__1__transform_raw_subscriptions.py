import pandas as pd 
import re
import logging
from airflow.exceptions import AirflowException


def filter_latest_subscription_event(input_file_path, output_file_path):
    """Get the most recent subscription event per item id.

    Args:
        input_file_path (str): file path, where file contains rows that represent a subscription event
        output_file_path (str): file path, where file contains rows that represent the latest subscription event (unique item id)
    """
    # read in files
    subscription_raw_df = pd.read_csv(input_file_path).fillna('')
    logging.info('Number of subscription rows: {}'.format(len(subscription_raw_df)))

    # sort subscription events by last update time, descending
    subscription_raw_df.sort_values(by='last_updated', ascending=False, inplace=True)

    # drop old subscription events
    subscription_latest_df = subscription_raw_df.drop_duplicates(subset=['item_id'], keep='first')
    logging.info('Number of unique subscription rows: {}'.format(len(subscription_latest_df)))

    # write output
    subscription_latest_df.to_csv(output_file_path, index=False)


def check_unique_item_id(input_file_path):
    # read in files
    subscription_df = pd.read_csv(input_file_path).fillna('')
    num_row = len(subscription_df)
    num_unique_item_id = len(subscription_df['item_id'].unique())

    if num_row != num_unique_item_id:
        error_message = 'Expected equal number of rows and unique ids,\
        recieved {num_row} rows and {num_unique_item_id} ids.'.format(
            num_row=num_row, 
            num_unique_item_id=num_unique_item_id
        )
        raise AirflowException(error_message)


def get_subscription_service(input_file_path, service_matching_file_path, output_file_path):
    """Extract the subscription service from the event description using a pre-determined set of rules.

    Removes any subscription events that do not have a service match.

    Args:
        input_file_path (str): file path, where file contains rows that represent a subscription
        service_matching_file_path (str): file path, where file contains rows that represent a matching rule for a service
        output_file_path (str): file path, where file contains rows that represent a subscription with a service match
    """
    # read in files
    subscription_raw_df = pd.read_csv(input_file_path).fillna('')
    service_matching_rule_df = pd.read_csv(service_matching_file_path).fillna('')
    logging.info('Number of subscription rows: {}'.format(len(subscription_raw_df)))
    logging.info('Number of service matching rules: {}'.format(len(service_matching_rule_df)))

    # for each subscription, get possible service matching rules
    subscription_with_service_rule_df = subscription_raw_df.merge(
        service_matching_rule_df, 
        how='inner',
        on='merchant_id'
    )
    logging.info('Number of subscription rows with service rule: {}'.format(len(subscription_with_service_rule_df)))

    # apply each rule to subscription text to determine if there is a service match
    subscription_with_service_rule_df['has_service_match'] = subscription_with_service_rule_df.apply(has_service_match, axis=1)
    logging.info('Number of subscription rows with service rule: {}'.format(len(subscription_with_service_rule_df)))

    # drop any subscriptions that do not have matches
    subscription_with_service_match_df = subscription_with_service_rule_df[subscription_with_service_rule_df['has_service_match']]
    logging.info('Number of subscription rows with service match: {}'.format(len(subscription_with_service_match_df)))

    # drop duplicate item id subscriptions if there are multiple service matches
    subscription_with_service_match_unique_df = subscription_with_service_match_df.drop_duplicates(subset=['item_id'])
    logging.info('Number of unique subscription rows with service match: {}'.format(len(subscription_with_service_match_unique_df)))

    # write output
    subscription_with_service_match_unique_df.to_csv(output_file_path, index=False)


def has_service_match(row):
    """Determine if subscription service matches body of text.

    Args:
        row (Series): a subscription row that contains input text, matching rule type, text to match on, and text to exclude

    Returns:
        bool: True if service rule is matched in text, otherwise False
    """
    # logging.info(row)
    item_id = row['item_id']
    input_text = row['description']
    text_match = row['text_match']
    text_exclude = row['text_exclude']
    matching = row['matching']
    logging.info(f'{item_id}: Checking "{input_text}" with "{text_match}"')
    
    # add check for exclude_text
    # check if match is at start of text
    if matching == 'S':
        if input_text[:len(text_match)] == text_match:
            logging.info('match')
            return True
    # check if match is within text
    elif matching == 'A':
        if text_match in input_text:
            logging.info('match')
            return True
    # check if match has regex match
    elif matching == 'R':
        if re.search(text_match, input_text):
            logging.info('match')
            return True
    
    logging.info('no match')
    return False
