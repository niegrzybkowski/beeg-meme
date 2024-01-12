#!/bin/bash

# refresh repo
cd /root/repo
git pull
cd /root

# refresh requirements
pip install -r processing/requirements.txt
pip install -r processing/requirements_cluster.txt
