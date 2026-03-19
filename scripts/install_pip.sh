#!/bin/bash
echo '=== OnusReport-AI: Installing Python packages ==='
apt-get update -qq && apt-get install -y --no-install-recommends python3-pip
pip3 install --break-system-packages pymysql httpx python-dotenv pyyaml pandas pyarrow boto3 duckdb
echo '=== Verifying ==='
python3 -c "import pymysql,httpx,dotenv,yaml,pandas,pyarrow,boto3,duckdb; print('ALL packages OK')"