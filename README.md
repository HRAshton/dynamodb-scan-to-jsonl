# dynamodb-scan-to-jsonl

Scans DynamoDB tables and writes the results to the existing jsonl files.

## Usage

1. Create a new repository.
2. Add this repository as a submodule.
3. Install dependencies with `pip install -r requirements.txt`.
4. Create `table-name.jsonl` files for each table you want to scan.
5. Run the script with `python3 ./dynamodb-full-table-scan-to-git/main.py`.

## Configuration

JSONL files should be placed in the current working directory.

The script reads configuration from the following environment variables:

- All standard AWS environment variables. See the [AWS CLI documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html).  
  Use either `AWS_PROFILE` or `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
  `AWS_REGION` to specify the AWS profile to use.
- SORT_KEYS: Whether to sort the keys in the JSON lines. Defaults to `true`.
- SORTABLE_PRIMARY_KEY: The primary key to sort by. Defaults to `None`
  (no sorting).
