import pandas as pd 
import re
import logging

MATCHING_RULE_START = 'S'
MATCHING_RULE_ANYWHERE = 'A'
MATCHING_RULE_REGEX = 'R'
VALID_MATCHING_RULES = set([MATCHING_RULE_START, MATCHING_RULE_ANYWHERE, MATCHING_RULE_REGEX])

SIGNAL_TYPE_CANCEL = 'Cancel'
SIGNAL_TYPE_SIGNUP = 'Signup'


def filter_latest_subscription_event(input_file_path, output_file_path):
    """Get the most recent subscription event per item id.

    Args:
        input_file_path (str): file path, where file contains rows that represent a subscription event
        output_file_path (str): file path, where file contains rows that represent the latest subscription event (unique item id)
    """
    # read in files
    subscription_df = pd.read_csv(input_file_path).fillna('')
    logging.info('Number of subscription rows: {}'.format(len(subscription_df)))

    # sort subscription events by last update time, descending
    subscription_df.sort_values(by='last_updated', ascending=False, inplace=True)

    # drop old subscription events
    subscription_latest_df = subscription_df.drop_duplicates(subset=['item_id'], keep='first')
    logging.info('Number of unique subscription rows: {}'.format(len(subscription_latest_df)))

    # write output
    subscription_latest_df.to_csv(output_file_path, index=False)


def check_service_matching_rules(input_file_path):
    """Check if service matching rules contains expected matching values.

    Args:
        input_file_path (str): file path, where file contains rows that represent a matching rule for a service

    Raises:
        AssertionException: Raise error if there are unexpected matching values
    """
    service_matching_rule_df = pd.read_csv(input_file_path).fillna('')

    used_matching_values = set(service_matching_rule_df['matching'])

    error_message = f'Expected {VALID_MATCHING_RULES} matching values,\
        recieved {used_matching_values} matching values in rules.'
    
    assert (used_matching_values.issubset(VALID_MATCHING_RULES)), error_message


def get_subscription_service(input_file_path, service_matching_file_path, output_file_path):
    """Extract the subscription service from the event description using a pre-determined set of rules.

    Removes any subscription events that do not have a service match.

    Args:
        input_file_path (str): file path, where file contains rows that represent a subscription
        service_matching_file_path (str): file path, where file contains rows that represent a matching rule for a service
        output_file_path (str): file path, where file contains rows that represent a subscription with a service match
    """
    # read in files
    subscription_df = pd.read_csv(input_file_path).fillna('')
    service_matching_rule_df = pd.read_csv(service_matching_file_path).fillna('')
    logging.info('Number of subscription rows: {}'.format(len(subscription_df)))
    logging.info('Number of service matching rules: {}'.format(len(service_matching_rule_df)))

    # for each subscription, get possible service matching rules
    subscription_with_service_rule_df = subscription_df.merge(
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
    subscription_with_service_match_unique_df.drop(
        labels=['has_service_match', 'text_match','text_exclude','matching'], 
        axis=1, 
        inplace=True)
    subscription_with_service_match_unique_df.to_csv(output_file_path, index=False)


def has_service_match(row):
    """Determine if subscription service matches body of text.

    Args:
        row (Series): a subscription row that contains input text, matching rule type, text to match on, and text to exclude

    Returns:
        bool: True if service rule is matched in text, otherwise False
    """

    item_id = row['item_id']
    input_text = row['description']
    text_match = row['text_match']
    text_exclude = row['text_exclude']
    matching = row['matching']
    logging.info(f'{item_id}: Use {matching} to check "{input_text}" with "{text_match}" ignoring "{text_exclude}"')
    
    # check if match is at start of text
    if matching == MATCHING_RULE_START:
        if input_text[:len(text_match)] == text_match:
            # exclude text is empty so match exists
            if len(text_exclude) == 0:
                logging.info('match')
                return True
            # if exclude text is part of rule, then check it
            elif len(text_exclude) > 0 and input_text[:len(text_exclude)] != text_exclude:
                logging.info('match')
                return True
    
    # check if match is anywhere in text
    elif matching == MATCHING_RULE_ANYWHERE:
        if text_match in input_text:
            # exclude text is empty so match exists
            if len(text_exclude) == 0:
                logging.info('match')
                return True
            # if exclude text is part of rule, then check it
            elif len(text_exclude) > 0 and text_exclude not in input_text:
                logging.info('match')
                return True
    
    # check for regex match
    elif matching == MATCHING_RULE_REGEX:
        if re.search(text_match, input_text):
            # exclude text is empty so match exists
            if len(text_exclude) == 0:
                logging.info('match')
                return True
            # if exclude text is part of rule, then check it
            elif len(text_exclude) > 0 and re.search(text_exclude, input_text) is None:
                logging.info('match')
                return True
    
    logging.info('no match')
    return False


def get_subscription_signal_type(input_file_path, output_file_path):
    """Extract the subscription signal type from the event description.

    Args:
        input_file_path (str): file path, where file contains rows that represent a subscription
        output_file_path (str): file path, where file contains rows that represent a subscription with signal type
    """
    # read in files
    subscription_df = pd.read_csv(input_file_path).fillna('')
    logging.info('Number of subscription rows: {}'.format(len(subscription_df)))

    # apply each rule to subscription text to determine if signal type
    subscription_df['signal_type'] = subscription_df.apply(match_signal_type, axis=1)

    # write output
    subscription_df.to_csv(output_file_path, index=False)


def match_signal_type(row):
    """Determine subscription signal type.

    Args:
        row (Series): a subscription row

    Returns:
        str: a signal type such as Signup, Cancel, etc
    """

    item_id = row['item_id']
    input_text = row['description']
    logging.info(f'{item_id}: Checking "{input_text}"')

    if 'cancel' in input_text.lower():
        return SIGNAL_TYPE_CANCEL
    return SIGNAL_TYPE_SIGNUP


def get_subscription_is_trial(input_file_path, output_file_path):
    """Extract the subscription trial status.

    Args:
        input_file_path (str): file path, where file contains rows that represent a subscription
        output_file_path (str): file path, where file contains rows that represent a subscription with trial status
    """
    # read in files
    subscription_df = pd.read_csv(input_file_path).fillna('')
    logging.info('Number of subscription rows: {}'.format(len(subscription_df)))

    # apply each rule to subscription text to determine if signal type
    subscription_df['is_trial'] = subscription_df.apply(is_trial, axis=1)
    count_by_is_trial = subscription_df.groupby(by=['is_trial'], dropna=False).count()['item_id']
    logging.info(count_by_is_trial)

    # write output
    subscription_df.to_csv(output_file_path, index=False)


def is_trial(row):
    """Determine if subscription is for a trial or not.

    Args:
        row (Series): a subscription row

    Returns:
        bool: True if subscription is a trial, otherwise False
    """

    item_id = row['item_id']
    input_text = row['description']
    logging.info(f'{item_id}: Checking "{input_text}"')

    if 'trial' in input_text.lower():
        return True
    elif 'free' in input_text.lower():
        return True
    
    return False


