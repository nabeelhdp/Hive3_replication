#!/bin/bash

###################################################################
# Script Name : hive3-repl.sh
# Description : Replicate Hive databases between two clusters. 
#               Supports both managed and external tables.
#               Auto detects a boostrap dump/load vs incremental.
#               Supports retries for failed incremental load.
# Args        : <database name>
# Authors     : Nabeel Moidu, Roy White
# Email       : nmoidu@cloudera.com, rwhite@cloudera.com
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
source ${THIS_DIR}/init_variables.sh

# ----------------------------------------------------------------------------
# Source common functions
#
source ${THIS_DIR}/misc_functions.sh
source ${THIS_DIR}/beeline_functions.sh


################ MAIN BEGINS HERE #########################

# Argument count should only be 1, and that should be the DBNAME
if [[ "$#" -ne 1 ]]; then
  script_usage
fi

[ -d ${TMP_DIR} ] || mkdir -p ${TMP_DIR} 
[ -d ${LOG_DIR} ] || mkdir -p ${LOG_DIR} 

SCRIPT_NAME=$(basename -- "$0")
DBNAME=""

# Validate input argument
if check_db_validity $1; then 
  # first argument is the db name if validity check is passed
  DBNAME=$1
else
  echo "ERROR: Database name ${DBNAME} not listed in env.sh. Aborting!"
  exit 1
fi

# location for log file
# This was moved from init-variables.sh here because DBNAME is set here
REPL_LOG_FILE="${LOG_DIR}/replication_${DBNAME}_${CURRENT_TIME}.log"

# Optional db level lock to avoid overlapping runs for same database.
# Defaults to false, i.e. no locking in place. 
# Use flock when invoking script as an alternative if needed.
lock_name=""
if [[ "${APPLY_DB_LOCK}" == "true" ]]; then
  lock_name=${SCRIPT_NAME}_${DBNAME}.lock
  check_instance_lock ${lock_name}
fi

trap ' trap_log_int ${lock_name} ' INT TERM
trap ' trap_log_exit ${lock_name} ' EXIT 

echo "===================================================================" >>${REPL_LOG_FILE}
printmessage " INFO: Initiating run to replicate ${DBNAME} to ${DBNAME}."
echo "==================================================================="  >>${REPL_LOG_FILE}
echo "For detailed logging, run tail -f on ${REPL_LOG_FILE}"

# Retrieve the current state of replication in the target cluster.
retrieve_current_target_repl_id

# If the REPL STATUS output is "NULL", it means the database has not been replicated.
# So a full dump will be generated.
if [[ "${LAST_REPL_ID}" == "NULL" ]]; then
  printmessage " INFO: No replication id detected at target. Full data dump dump needs to be initiated."
  printmessage " INFO: Database ${DBNAME} is being synced for the first time. Initiating full dump."
  
  # Generate bootstrap dump at source
  gen_bootstrap_dump_source
  
  # DUMP_TXID is set in the above function to the current transaction ID at source cluster for the database
  printmessage " INFO: Source transaction id at the time of dump: |${DUMP_TXID}|"

  # If dump is generated successfully, a proper integer value is returned.
  if [[ "${DUMP_TXID}" -gt 0 ]]; then
    printmessage " INFO: Initiating data load at target cluster on database ${DBNAME}."

    # Point to corresponding HQL file depending on whether external tables are to be included or not
    if [[ "${INCLUDE_EXTERNAL_TABLES}" == "true" ]]; then
      printmessage " INFO: External tables included. This may trigger distcp jobs in background."
      HQL_FILE=${EXT_LOAD_HQL}
    else 
      printmessage " INFO: External tables not included."
      HQL_FILE=${LOAD_HQL}
    fi 
    
    # Override the variable INCR_RERUN and set it to 1 to disable retries for bootstrap load. 
    INCR_RERUN=1
    # Now the source cluster has generated a full dump of the database. 
    # Replay the full dump on the target cluster database.
    if replay_dump_at_target ${HQL_FILE}; then 
      printmessage " INFO: Data load at target cluster completed. Verifying...." 
      retrieve_post_load_target_repl_id
      if [[ "${POST_LOAD_REPL_ID}" == "${DUMP_TXID}" ]] ; then
        printmessage " INFO: Database replication completed SUCCESSFULLY. Last transaction id at target is |${POST_LOAD_REPL_ID}|"
      elif [[ "${POST_LOAD_REPL_ID}" == "NULL" ]] ; then
        printmessage " ERROR: Database replication FAILED. No transactions have been applied in this run."
     elif [[ ${POST_LOAD_REPL_ID} -lt ${DUMP_TXID} ]] ; then
        printmessage " ERROR: Transaction event ID in target is behind the event ID at source at time of current dump."
        printmessage " ERROR: This will require a cleanup of the partially loaded database in target."
     elif [[ ${POST_LOAD_REPL_ID} -gt ${DUMP_TXID} ]] ; then
        printmessage " WARN: Transaction event ID in target is ahead of the event ID at source at time of current dump."
        printmessage " WARN: This may happen if there is another REPL LOAD in progress with a later copy of the source dump"
     else
        printmessage " ERROR: Unable to verify database replication! Post Load repl id: |${POST_LOAD_REPL_ID}|"
        printmessage " ERROR: Source repl id: |${DUMP_TXID}|"    
        exit 1
      fi
    else 
      printmessage " ERROR: Data load at target cluster failed" 
      echo -e "See ${REPL_LOG_FILE} for details. Exiting!" 
      exit 1
    fi 
        
  # If DUMP_TXID is anything but a proper number, 
  # it indicates a failure in geenerating the source dump. Exit.
  else  
    printmessage " ERROR: Invalid transaction id returned from Source : |${DUMP_TXID}|"
    # Print error message to console
    local errormsg=$(egrep -e ^Error -e ^ERROR -e FAILED ${REPL_LOG_FILE} | tail -1)
    echo "${errormsg}"
    printmessage " ERROR: Unable to generate full dump for database ${DBNAME}. Exiting!."
    exit 1
  fi

# If the database at target cluster already has some transaction replayed, 
# trigger the source dump as an incremental dump.
elif [[ ${LAST_REPL_ID} =~ ${TXN_ID_REGEX} ]] ; then
  # adding | | around variable names to detect whitespace in parsed output
  printmessage " INFO: Database ${DBNAME} transaction ID at target is currently |${LAST_REPL_ID}|"
  
  # Generate incremental dump at source
  gen_incremental_dump_source

  if [[ "${DUMP_TXID}" -gt 0 ]]; then
    printmessage " INFO: Initiating REPL LOAD at destination cluster to replicate ${DBNAME} to transaction id |${DUMP_TXID}|."
    
    # Point to corresponding HQL file depending on whether external tables are to be included or not
    if [[ "${INCLUDE_EXTERNAL_TABLES}" == "true" ]]; then
      printmessage " INFO: External tables included. This may trigger distcp jobs in background."
      HQL_FILE=${EXT_LOAD_HQL}
    else 
      printmessage " INFO: External tables not included."
      HQL_FILE=${LOAD_HQL}
    fi 
        
    # Now the source cluster has generated a dump of the database. 
    # Replay the dump on the target cluster database.
    if replay_dump_at_target ${HQL_FILE}; then 
      printmessage " INFO: Data load at target cluster completed. Verifying...." 
      retrieve_post_load_target_repl_id
      if [[ "${POST_LOAD_REPL_ID}" == "${DUMP_TXID}" ]] ; then
        printmessage " INFO: Database replication completed SUCCESSFULLY. Last transaction id at target is |${POST_LOAD_REPL_ID}|"
      elif [[ "${POST_LOAD_REPL_ID}" == "${LAST_REPL_ID}" ]] ; then
        printmessage " ERROR: Database replication FAILED. No transactions have been applied in this run."
      elif [[ ${POST_LOAD_REPL_ID} -lt ${DUMP_TXID} ]] ; then
        printmessage " WARN: Replication from source to target is incomplete."
        printmessage " WARN: Transaction event ID in target is behind the event ID at source at time of current dump."
     elif [[ ${POST_LOAD_REPL_ID} -gt ${DUMP_TXID} ]] ; then
        printmessage " WARN: Transaction event ID in target is ahead of the event ID at source at time of current dump."
        printmessage " WARN: This may happen if there is another REPL LOAD in progress with a later copy of the source dump"
      else
        printmessage " ERROR: Unable to verify database replication! Post Load repl id: |${POST_LOAD_REPL_ID}|"
        printmessage " ERROR: Source dump repl id: |${DUMP_TXID}|"    
        exit 1
      fi
    else 
      printmessage " ERROR: Data load at target cluster failed" 
      echo -e "See ${REPL_LOG_FILE} for details. Exiting!" 
      exit 1
    fi 
  else
    printmessage " ERROR: Invalid transaction id returned from Source : |${DUMP_TXID}|"
    # Print error message to console
    local errormsg=$(egrep -e ^Error -e ^ERROR -e FAILED ${REPL_LOG_FILE} | tail -1)
    echo "${errormsg}"
    printmessage " ERROR: Unable to generate incremental dump for database ${DBNAME}. Exiting!."
    exit 1
  fi
else
  printmessage " ERROR: Invalid value for last replicated transaction id: ${LAST_REPL_ID}. Database dump failed"
  echo -e "See ${REPL_LOG_FILE} for details. Exiting!"
  exit 1
fi
