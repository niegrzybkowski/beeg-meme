#!/bin/bash

# refresh repo
cd /root/repo
git pull
cd /root

# refresh requirements
pip install -r repo/processing/requirements.txt
pip install -r repo/processing/requirements_cluster.txt
