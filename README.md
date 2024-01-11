# Academic Paper Scraper and AWS S3 Uploader

This Python script is designed to query academic papers from various *rxiv* repositories (medrxiv, biorxiv, chemrxiv), append new records to master JSONL files, and upload these files to an AWS S3 bucket.

## Prerequisites

- Python 3.11.2
- boto3==1.34.16
- paperscraper==0.2.9
