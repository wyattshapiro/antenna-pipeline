import pandas as pd 
import re

def identify_service_for_subscription(input_file_path, matching_file_path, output_file_path):
    """Extract the subscription service from the event description using a pre-determined set of rules.

    Args:
        input_file_path (str): file path, where file contains rows that represent a subscription
        matching_file_path (str): file path, where file contains rows that represent a matching rule for a service
        output_file_path (str): file path, where file contains rows that represent a subscription with a service match
    """
    # read in files
    raw_subscription_df = pd.read_csv(input_file_path).fillna('')
    matching_rule_df = pd.read_csv(matching_file_path).fillna('')
    print('Number of subscription rows: {}'.format(len(raw_subscription_df)))
    print('Number of service matching rules: {}'.format(len(matching_rule_df)))

    # for each subscription, get possible service matching rules
    subscription_with_service_rule_df = raw_subscription_df.merge(
        matching_rule_df, 
        how='inner',
        on='merchant_id'
    )
    print('Number of subscription rows with service rule: {}'.format(len(subscription_with_service_rule_df)))

    # apply each rule to text
    subscription_with_service_rule_df['has_service_match'] = subscription_with_service_rule_df.apply(identify_service, axis=1)
    print('Number of subscription rows with service rule: {}'.format(len(subscription_with_service_rule_df)))

    # drop any rows that do not have matches
    subscription_with_service_match_df = subscription_with_service_rule_df[subscription_with_service_rule_df['has_service_match']]
    print('Number of subscription rows with service match: {}'.format(len(subscription_with_service_match_df)))

    # drop duplicates subscriptions
    unique_subscription_with_service_match_df = subscription_with_service_match_df.drop_duplicates(subset=['item_id'])
    print('Number of unique subscription rows with service match: {}'.format(len(unique_subscription_with_service_match_df)))

    # write output
    subscription_with_service_match_df.to_csv(output_file_path, index=False)


def identify_service(row):
    """Identify a subscription service in a body of text.

    Args:
        row (Series): a subscription row that contains input text, matching rule type, text to match on, and text to exclude

    Returns:
        bool: True if service rule is matched in text, otherwise False
    """
    # print(row)
    input_text = row['description']
    text_match = row['text_match']
    text_exclude = row['text_exclude']
    matching = row['matching']
    print(f'Checking "{input_text}" for "{text_match}"')
    
    # add check for exclude_text
    # check if match is at start of text
    if matching == 'S':
        if input_text[:len(text_match)] == text_match:
            print('match')
            return True
    # check if match is within text
    elif matching == 'A':
        if text_match in input_text:
            print('match')
            return True
    # check if match has regex match
    elif matching == 'R':
        if re.search(text_match, input_text):
            print('match')
            return True
    
    print('no match')
    return False
