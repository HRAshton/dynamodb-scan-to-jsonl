"""
Scans all tables in the DynamoDB and saves the output to the .jsonl files.

It reads the configuration from the environment variables:
- JSONLS_DIR: The directory where the .jsonl files are stored.
- SORT_KEYS: Whether to sort the keys in the JSON lines.
- SORTABLE_PRIMARY_KEY: Name of the primary key to sort the items by.

If run with the default configuration, it will:
1. Find all .jsonl files in the parent directory of this repository.
2. Calculate required table names from the file names.
3. Scan all tables and save the output to the corresponding files.
"""

import json
import logging
import os
import sys
from typing import Any

import boto3

logger = logging.getLogger()

# The directory where the .jsonl files are stored.
# Default: parent directory of this repository.
JSONLS_DIR = os.environ.get('JSONLS_DIR', os.path.join(os.path.realpath(__file__),
                                                       os.pardir,
                                                       os.pardir))

# Whether to sort the keys in the JSON lines.
# Default: true
SORT_KEYS = os.environ.get('SORT_KEYS', 'true').lower() == 'true'

# Name of the primary key to sort the items by.
# If not specified, the items will not be sorted.
# Default: None
SORTABLE_PRIMARY_KEY = os.environ.get('SORTABLE_PRIMARY_KEY', 'userId')


def get_tables_with_files() -> dict[str, str]:
    """Returns all .jsonl files from the parent directory"""
    return {
        name.replace('.jsonl', ''): os.path.abspath(os.path.join(JSONLS_DIR, name))
        for name in os.listdir(JSONLS_DIR)
        if name.endswith('.jsonl')
    }


def scan_table(client, table_name: str) -> list[dict[str, Any]]:
    """Scans the table and writes the output to the file"""
    paginator = client.get_paginator('scan')
    response_iterator = paginator.paginate(TableName=table_name)
    items = []
    for page in response_iterator:
        items.extend(page['Items'])
        logger.info('Scanned %d items from table %s', len(items), table_name)

    logger.info('Table %s scanned successfully, %d items', table_name, len(items))
    return items


def save_items_to_file(items: list[dict], output_file: str) -> None:
    """Saves the items to the file"""
    if SORTABLE_PRIMARY_KEY:
        # Each field is a dictionary, so we need to stringify it.
        items.sort(key=lambda x: str(x.get(SORTABLE_PRIMARY_KEY)))

    with open(output_file, 'w', encoding='utf-8') as file:
        for item in items:
            file.write(json.dumps(item, sort_keys=SORT_KEYS) + '\n')


def main():
    """Scans all tables and saves the output to the files"""
    client = boto3.Session().client('dynamodb')
    tables_with_files = get_tables_with_files()
    for input_table, output_file in tables_with_files.items():
        logger.info('Scanning table %s and saving to file %s', input_table, output_file)
        items = scan_table(client, input_table)
        save_items_to_file(items, output_file)

    logger.info('All tables scanned successfully')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    logger.info(json.dumps({
        'JSONLS_DIR': JSONLS_DIR,
        'SORT_KEYS': SORT_KEYS,
        'SORTABLE_PRIMARY_KEY': SORTABLE_PRIMARY_KEY,
    }, indent=2))

    if len(sys.argv) > 1:
        logger.info('Using AWS profile: %s', sys.argv[1])
        os.environ['AWS_PROFILE'] = sys.argv[1]
    elif 'AWS_PROFILE' in os.environ:
        logger.info('Using AWS profile: %s', os.environ['AWS_PROFILE'])
    else:
        logger.info('Using default AWS profile')

    main()
