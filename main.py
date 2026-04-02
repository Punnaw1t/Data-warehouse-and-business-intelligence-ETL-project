"""Entry point for the ETL application

Sample usage:
python main.py \
  --source /opt/data/transaction.csv \
  --destination /opt/data/out.parquet
"""

from cli.cli import main

if __name__ == "__main__":
    main()
