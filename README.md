# Academic Paper Scraper and AWS S3 Uploader

This Python script is designed to maintain  *rxiv* repository (chemrxiv, biorxiv, medrxiv) metadata.  It downloads the latest daily paper records and appends them to master JSONL files. These are uploaded to an AWS S3 bucket (currently s3://astrowafflerp/*rxiv.jsonl)

Use command line: aws s3 cp s3://astrowafflerp/chemrxiv.jsonl [chem/bio/med]rxiv.jsonl --request-payer

## Prerequisites

- Python 3.11.2
- boto3==1.34.16
- paperscraper==0.2.9
