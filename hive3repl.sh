#!/bin/bash

###################################################################
# Script Name : hive3-repl.sh
# Description : Replicate Hive databases between two clusters. 
#               Supports both managed and external tables.
# Args        :
# Author      : Nabeel Moidu
# Email       : nmoidu@cloudera.com
###################################################################

# Add seconds inbuilt bash counter to track time of run.
SECONDS=0

# Directory where script runs from
THIS_DIR=$(dirname "$0")

# ----------------------------------------------------------------------------
# Source environment variables from file
#
source ${THIS_DIR}/env.sh

# ----------------------------------------------------------------------------
# Set global variables that should not be changed by user
#
source ${THIS_DIR}/init-variables.sh

# ----------------------------------------------------------------------------
# Source common functions
#
source ${THIS_DIR}/misc-functions.sh
source ${THIS_DIR}/beeline-functions.sh


################ MAIN BEGINS HERE #########################

# Argument count should only be 1, and that should be the DBNAME
if [[ "$#" -ne 1 ]]; then
  script_usage
fi

[ -d ${TMP_DIR} ] || mkdir -p ${TMP_DIR} 
[ -d ${LOG_DIR} ] || mkdir -p ${LOG_DIR} 

SCRIPT_NAME=$(basename -- "$0")

# first argument is the db name 
DBNAME=$1

# location for log file
# This was moved from init-variables.sh here because DBNAME is set here
REPL_LOG_FILE="${LOG_DIR}/replication_${DBNAME}_${CURRENT_TIME}.log"

# Optional db level lock to avoid overlapping runs for same database.
# Defaults to false, i.e. no locking in place. 
# Use flock when invoking script as an alternative if needed.
if [[ "${APPLY_DB_LOCK}" == "true" ]]
then
  lock_name=${SCRIPT_NAME}_${DBNAME}.lock
  check_instance_lock ${lock_name}
fi

trap { trap_log_int ${lock_name}} INT TERM
trap { trap_log_exit ${lock_name}} EXIT 

echo "===================================================================" >>${REPL_LOG_FILE}
printmessage "Initiating run to replicate ${DBNAME} to ${DBNAME}."
echo "==================================================================="  >>${REPL_LOG_FILE}
echo " For detailed logging, run tail -f on ${REPL_LOG_FILE}"

# Retrieve the current state of replication in the target cluster.
retrieve_current_target_repl_id

# If the REPL STATUS output is "NULL", it means the database has not been replicated.
# So a full dump will be generated.
if [[ "${last_repl_id}" == "NULL" ]]; then
  printmessage "No replication id detected at target. Full data dump dump needs to be initiated."
  printmessage "Database ${DBNAME} is being synced for the first time. Initiating full dump."
  
  # Point to corresponding HQL file depending on whether external tables are to be included or not
  if [[ "${INCLUDE_EXTERNAL_TABLES}" == "true" ]]; then
    printmessage "Including external tables in full dump"
    gen_bootstrap_dump_source ${EXT_BOOTSTRAP_HQL}
  else 
    printmessage "Skipping external tables in full dump"
    gen_bootstrap_dump_source ${BOOTSTRAP_HQL}
  fi 
  
  # dump_txid is set in the above function to the current transaction ID at source cluster for the database
  source_latest_txid=${dump_txid}
  printmessage "Source transaction id: |${source_latest_txid}|"

  # If dump is generated successfully, a proper integer value is returned.
  if [[ "${source_latest_txid}" -gt 0 ]]; then
    printmessage "Database ${DBNAME} full dump has been generated at |${SOURCE_HDFS_PREFIX}${dump_path}|."
    printmessage "The current transaction ID at source is |${source_latest_txid}|"
    printmessage "There are ${source_latest_txid} transactions to be synced in this run."
    printmessage "Initiating data load at target cluster on database ${DBNAME}."

    # Point to corresponding HQL file depending on whether external tables are to be included or not
    if [[ "${INCLUDE_EXTERNAL_TABLES}" == "true" ]]; then
      printmessage "External tables included. This may trigger distcp jobs in background."
      HQL_FILE=${EXT_LOAD_HQL}
    else 
      printmessage "External tables not included."
      HQL_FILE=${LOAD_HQL}
    fi 
    
    # Override the variable INCR_RERUN and set it to 1 to disable retries for bootstrap load. 
    INCR_RERUN=1
    # Now the source cluster has generated a full dump of the database. 
    # Replay the full dump on the target cluster database.
    if replay_dump_at_target ${HQL_FILE}; then 
      printmessage "Data load at target cluster completed. Verifying...." 
      retrieve_post_load_target_repl_id
      if [[ "${post_load_repl_id}" == "${source_latest_txid}" ]] ; then
        printmessage "Database replication completed SUCCESSFULLY. Last transaction id at target is |${post_load_repl_id}|"
      elif [[ "${post_load_repl_id}" == "NULL" ]] ; then
        printmessage "Database replication FAILED. No transactions have been applied in this run."
        # TODO - cleanup directories leftover during failed replication (if any) 
      elif [[ ${post_load_repl_id} -gt ${source_latest_txid} ]] ; then
        printmessage "Transaction event ID in target is ahead of the event ID at source at time of current dump."
        printmessage "This may happen if there is another REPL LOAD in progress with a later copy of the source dump"
     else
        printmessage "Unable to verify database replication! Post Load repl id: |${post_load_repl_id}|"
        printmessage "Source repl id: |${source_latest_txid}|"    
        exit 1
      fi
    else 
      printmessage "Data load at target cluster failed" 
      echo -e "See ${REPL_LOG_FILE} for details. Exiting!" 
      exit 1
    fi 
        
  # If source_latest_txid is anything but a proper number, 
  # it indicates a failure in geenerating the source dump. Exit.
  else
    printmessage "Unable to generate full dump for database ${DBNAME}. Exiting!."
    exit 1
  fi

# If the database at target cluster already has some transaction replayed, 
# trigger the source dump as an incremental dump.
elif [[ "${last_repl_id}" =~ "${re}" ]] ; then
  printmessage "Database ${DBNAME} transaction ID at target is currently |${last_repl_id}|"

  # dump generation command returns latest transaction id at source
  if [[ "${INCLUDE_EXTERNAL_TABLES}" == "true" ]]; then
    printmessage "Including external tables in incremental dump"
    gen_incremental_dump_source ${EXT_INC_DUMP_HQL}
  else 
    printmessage "Skipping external tables in incremental dump"
    gen_incremental_dump_source ${INC_DUMP_HQL}
  fi 
 
  # dump_txid is set in the above function to the current transaction ID at source cluster for the database
  source_latest_txid=${dump_txid}
  printmessage "The current transaction ID at source is |${source_latest_txid}|"

  if [[ "${source_latest_txid}" -gt 0 ]]; then
    printmessage "Database ${DBNAME} incremental dump has been generated at |${SOURCE_HDFS_PREFIX}${dump_path}|."
    # the calculation of txn_count below doesn't match with numEvents that show up in the 
    #txn_count=$((${source_latest_txid} - ${last_repl_id}))
    printmessage "Initiating REPL LOAD at destination cluster to replicate ${DBNAME} to transaction id |${source_latest_txid}|."
    
    # Point to corresponding HQL file depending on whether external tables are to be included or not
    if [[ "${INCLUDE_EXTERNAL_TABLES}" == "true" ]]; then
      printmessage "External tables included. This may trigger distcp jobs in background."
      HQL_FILE=${EXT_LOAD_HQL}
    else 
      printmessage "External tables not included."
      HQL_FILE=${LOAD_HQL}
    fi 
        
    # Now the source cluster has generated a dump of the database. 
    # Replay the dump on the target cluster database.
    if replay_dump_at_target ${HQL_FILE}; then 
      printmessage "Data load at target cluster completed. Verifying...." 
      retrieve_post_load_target_repl_id
      if [[ "${post_load_repl_id}" == "${source_latest_txid}" ]] ; then
        printmessage "Database replication completed SUCCESSFULLY. Last transaction id at target is |${post_load_repl_id}|"
      elif [[ "${post_load_repl_id}" == "${last_repl_id}" ]] ; then
        printmessage "Database replication FAILED. No transactions have been applied in this run."
      elif [[ ${post_load_repl_id} -gt ${source_latest_txid} ]] ; then
        printmessage "Transaction event ID in target is ahead of the event ID at source at time of current dump."
        printmessage "This may happen if there is another REPL LOAD in progress with a later copy of the source dump"
      else
        printmessage "Unable to verify database replication! Post Load repl id: |${post_load_repl_id}|"
        printmessage "Source repl id: |${source_latest_txid}|"    
        exit 1
      fi
    else 
      printmessage "Data load at target cluster failed" 
      echo -e "See ${repl_log_file} for details. Exiting!" 
      exit 1
    fi 
        
  else
    printmessage "Invalid transaction id returned from Source : |${source_latest_txid}|"
    printmessage "Unable to generate incremental dump for database ${DBNAME}. Exiting!."
    exit 1
  fi

else
  printmessage "Invalid value for last replicated transaction id: ${last_repl_id}. Database dump failed"
  echo -e "See ${repl_log_file} for details. Exiting!"
  exit 1
fi
