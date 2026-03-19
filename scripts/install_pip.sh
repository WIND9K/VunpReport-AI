#!/bin/bash
# install_pip.sh — Cài Python packages cho OnusReport-AI trong Docker container
# Chạy: docker exec -u root openclaw-openclaw-gateway-1 bash /home/node/learn/OnusReport-AI/scripts/install_pip.sh

echo "=== OnusReport-AI: Installing Python packages ==="

apt-get update -qq
apt-get install -y --no-install-recommends python3-pip

pip3 install --break-system-packages \
  pymysql \
  httpx \
  python-dotenv \
  pyyaml \
  pandas \
  pyarrow \
  boto3 \
  duckdb

echo "=== Done! Verifying ==="
python3 -c "import pymysql, httpx, dotenv, yaml; print('Core packages OK')"
python3 -c "import pandas, pyarrow; print('Data packages OK')"
