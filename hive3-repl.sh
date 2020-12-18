#!/bin/bash

###################################################################
# Script Name : hive3-repl.sh
# Description : Replicate Hive databases between two clusters. 
#               Supports both managed and external tables.
# Args        :
# Author      : Nabeel Moidu
# Email       : nmoidu@cloudera.com
###################################################################

# ----------------------------------------------------------------------------
# Source environment variables from file
#
source ./env.sh

# ----------------------------------------------------------------------------
# Source common functions
#
source ./repl-common.sh
source ./beeline-functions.sh

trap trap_log_int INT
trap trap_log_exit EXIT


################ MAIN BEGINS HERE #########################

# Argument count should be either 1 or 2
if [[ "$#" > 2 ]] || [[ "$#" < 1 ]]; then
  script_usage
fi

# If argument count is 1 or 2, the first argument is the db name 
dbname=$1
# By default, loglevel is INFO
loglevel="INFO"

# If second argument exists, it should be DEBUG, else ignore
if [[ "$2" == "DEBUG" ]]; then
  printmessage "Enabling DEBUG output"
  loglevel="DEBUG" 
  beeline_opts="--verbose=true --showHeader=true --silent=false"
fi

# Validate dbname provided against list of valid names specified in env.sh
dbvalidity="0"
for val in ${dblist}; do
    if [[ ${val} == ${dbname} ]]; then
      dbvalidity="1"
    fi
done

if [[ ${dbvalidity} == "0" ]]; then
  printmessage "Invalid target database name specified. Falling back to source name."
  dbname=${dbname}
fi

printmessage "==================================================================="
printmessage "Initiating run to replicate ${dbname} to ${dbname} "
printmessage "==================================================================="

# Regex to detect if transaction ID is number
re='^[0-9]+$'

# For one run of this script, we expect only one dump path.
# Hence declaring it as global var to return from functions.
dump_path=""
dump_txid=""

last_repl_id=""
post_load_repl_id=""

# Retrieve the current state of replication in the target cluster.
retrieve_current_target_repl_id

if [[ ${last_repl_id} == "NULL" ]]; then
  printmessage "No replication id detected at target. Full data dump dump needs to be initiated."
  printmessage "Database ${dbname} is being synced for the first time. Initiating full dump."
  
  # dump generation command returns latest transaction id at source
  if [[ ${include_external_tables} == 'true' ]]; then
    printmessage "Including external tables in full dump"
    gen_bootstrap_dump_source ${EXT_BOOTSTRAP_HQL}
  else 
    printmessage "Skipping external tables in full dump"
    gen_bootstrap_dump_source ${BOOTSTRAP_HQL}
  fi 
  
  source_latest_txid=${dump_txid}
  printmessage "Source transaction id: |${source_latest_txid}|"

  if [[ ${source_latest_txid} > 0 ]]; then
    printmessage "Database ${dbname} full dump has been generated at |${source_hdfs_prefix}${dump_path}|."
    printmessage "The current transaction ID at source is |${source_latest_txid}|"
    printmessage "There are ${source_latest_txid} transactions to be synced in this run."
    printmessage "Initiating data load at target cluster on database ${dbname}."

    if [[ ${include_external_tables} == 'true' ]]; then
      printmessage "External tables included. This may trigger distcp jobs in background."
      HQL_FILE=${EXT_INC_DUMP_HQL}
    else 
      printmessage "External tables not included."
      HQL_FILE=${INC_DUMP_HQL}
    fi 
    
    if replay_dump_at_target ${HQL_FILE}; then 
      printmessage "Data load at target cluster completed. Verifying...." 
      retrieve_post_load_target_repl_id
      if [[ ${post_load_repl_id} == ${source_latest_txid} ]] ; then
        printmessage "Database replication completed SUCCESSFULLY. Last transaction id at target is |${post_load_repl_id}|"
      else
        printmessage "Invalid latest transaction id returned from Source : |${source_latest_txid}|"
        printmessage "Unable to generate incremental dump for database ${dbname}. Exiting!." && exit 1
      fi
    else 
      printmessage "Data load at target cluster failed" 
      echo -e "See ${repl_log_file} for details. Exiting!" 
      exit 1
    fi 
    
  
  else
    printmessage "Unable to generate full dump for database ${dbname}. Exiting!."
    exit 1
  fi

elif [[ ${last_repl_id} =~ ${re} ]] ; then
  printmessage "Database ${dbname} transaction ID at target is currently |${last_repl_id}|"

  
  # dump generation command returns latest transaction id at source
  if [[ ${include_external_tables} == 'true' ]]; then
    printmessage "Including external tables in incremental dump"
    gen_incremental_dump_source ${EXT_INC_DUMP_HQL}
  else 
    printmessage "Skipping external tables in incremental dump"
    gen_incremental_dump_source ${INC_DUMP_HQL}
  fi 
  
  source_latest_txid=${dump_txid}
  printmessage "The current transaction ID at source is |${source_latest_txid}|"

  if [[ ${source_latest_txid} > 0 ]]; then
    printmessage "Database ${dbname} incremental dump has been generated at |${source_hdfs_prefix}${dump_path}|."
    txn_count=$((${source_latest_txid} - ${last_repl_id}))
    printmessage "There are ${txn_count} transactions to be synced in this run."
    
    if [[ ${include_external_tables} == 'true' ]]; then
      printmessage "External tables included. This may trigger distcp jobs in background."
      HQL_FILE=${EXT_INC_DUMP_HQL}
    else 
      printmessage "External tables not included."
      HQL_FILE=${INC_DUMP_HQL}
    fi 
        
    if replay_dump_at_target ${HQL_FILE}; then 
      printmessage "Data load at target cluster completed. Verifying...." 
      retrieve_post_load_target_repl_id
      if [[ ${post_load_repl_id} == ${source_latest_txid} ]] ; then
        printmessage "Database replication completed SUCCESSFULLY. Last transaction id at target is |${post_load_repl_id}|"
      else
        printmessage "Unable to verify database replication! Post Load repl id: |${post_load_repl_id}|"
        printmessage "Source repl id: |${source_latest_txid}|"    exit 1
      fi
    else 
      printmessage "Data load at target cluster failed" 
      echo -e "See ${repl_log_file} for details. Exiting!" 
      exit 1
    fi 
        
  else
    printmessage "Invalid transaction id returned from Source : |${source_latest_txid}|"
    printmessage "Unable to generate incremental dump for database ${dbname}. Exiting!."
    exit 1
  fi

else
  printmessage "Invalid value for last replicated transaction id: ${last_repl_id}. Database dump failed"
  echo -e "See ${repl_log_file} for details. Exiting!"
  exit 1
fi
