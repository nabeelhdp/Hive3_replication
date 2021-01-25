#!/bin/bash

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


# Set unique value for logs and output directory based on time of script run
CURRENT_TIME=$(date +"%Y_%m_%d_%I_%M_%p")

# Directory where script runs from
THIS_DIR=$(dirname "$0")

# Location of log files for each run. 
LOG_DIR="${THIS_DIR}/logs"

# Location to place outputs of individual beeline commands run during replication
TMP_DIR="${THIS_DIR}/tmp/run_${CURRENT_TIME}"

# Folder containing HiveQL scripts
HQL_DIR="${THIS_DIR}/HQL"

# Folder to put the lock file
RUN_DIR="${THIS_DIR}/run"

# Locations for the various Hive QL scripts for each action.
INC_DUMP_HQL="${HQL_DIR}/repldump.hql"
BOOTSTRAP_HQL="${HQL_DIR}/replbootstrap.hql"
EXT_INC_DUMP_HQL="${HQL_DIR}/replextdump.hql"
EXT_BOOTSTRAP_HQL="${HQL_DIR}/replextbootstrap.hql"
LOAD_HQL="${HQL_DIR}/replload.hql"
EXT_LOAD_HQL="${HQL_DIR}/replextload.hql"
STATUS_HQL="${HQL_DIR}/replstatus.hql"
INITREPLCHANGEMANAGER_HQL="${HQL_DIR}/initReplChangeManager.hql"

# beeline options to be passed. Do not change this as the output parsing will break
# if this is changed.
beeline_opts="--verbose=true --showHeader=true --silent=false"
