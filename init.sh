#!/bin/bash

# By default, loglevel is INFO
loglevel="INFO"

# For one run of this script, we expect only one dump path.
# Hence declaring it as global var to return from functions.
dump_path=""
dump_txid=""

# current replication id at target before we begin replication
last_repl_id=""

# current replication id at target after we complete replication
post_load_repl_id=""

# Regex to detect if transaction ID is number
re='^[0-9]+$'

# Folder containing HiveQL scripts
HQL_DIR="./HQL"

# Locations for the various Hive QL scripts for each action.
INC_DUMP_HQL="${HQL_DIR}/repldump.hql"
BOOTSTRAP_HQL="${HQL_DIR}/replbootstrap.hql"
EXT_INC_DUMP_HQL="${HQL_DIR}/replextdump.hql"
EXT_BOOTSTRAP_HQL="${HQL_DIR}/replextbootstrap.hql"
LOAD_HQL="${HQL_DIR}/replload.hql"
EXT_LOAD_HQL="${HQL_DIR}/replextload.hql"
STATUS_HQL="${HQL_DIR}/replstatus.hql"

# location for log file
repl_log_file="${LOG_DIR}/replication_${current_time}.log"

# This is the default setting for beeline. To change, 
# use DEBUG option when running the script. 
# Do not change here, as it breaks output parsing
beeline_opts="--verbose=false --showHeader=false --silent=true"
