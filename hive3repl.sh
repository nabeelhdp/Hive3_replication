#!/bin/bash

###################################################################
# Script Name : hive3-repl.sh
# Description : Replicate Hive databases between two clusters. 
#               Supports both managed and external tables.
# Args        :
# Author      : Nabeel Moidu
# Email       : nmoidu@cloudera.com
###################################################################

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

trap trap_log_int INT TERM
trap trap_log_exit EXIT 


################ MAIN BEGINS HERE #########################

# Argument count should be 1, the dbname
if [[ "$#" -ne 1 ]]; then
  script_usage
fi

[ -d ${TMP_DIR} ] || mkdir -p ${TMP_DIR} 
[ -d ${LOG_DIR} ] || mkdir -p ${LOG_DIR} 

script_name=$(basename -- "$0")

# first argument is the db name 
dbname=$1

# location for log file
repl_log_file="${LOG_DIR}/replication_${dbname}_${current_time}.log"

lock_name=${script_name}_${dbname}.lock
check_instance_lock ${lock_name}

# Validate dbname provided against list of valid names specified in env.sh
dbvalidity="0"
for val in ${dblist}; do
    if [[ ${val} == ${dbname} ]]; then
      dbvalidity="1"
    fi
done

if [[ ${dbvalidity} == "0" ]]; then
  printmessage "Invalid target database name specified. Exiting!"
  exit 1
fi

echo "===================================================================" >>${repl_log_file}
printmessage "Initiating run to replicate ${dbname} to ${dbname}."
echo "==================================================================="  >>${repl_log_file}
echo " For detailed logging, run tail -f on ${repl_log_file}"

# Retrieve the current state of replication in the target cluster.
retrieve_current_target_repl_id

# If the REPL STATUS output is "NULL", it means the database has not been replicated.
# So a full dump will be generated.
if [[ ${last_repl_id} == "NULL" ]]; then
  printmessage "No replication id detected at target. Full data dump dump needs to be initiated."
  printmessage "Database ${dbname} is being synced for the first time. Initiating full dump."
  
  # Point to corresponding HQL file depending on whether external tables are to be included or not
  if [[ ${include_external_tables} == 'true' ]]; then
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
  if [[ ${source_latest_txid} > 0 ]]; then
    printmessage "Database ${dbname} full dump has been generated at |${source_hdfs_prefix}${dump_path}|."
    printmessage "The current transaction ID at source is |${source_latest_txid}|"
    printmessage "There are ${source_latest_txid} transactions to be synced in this run."
    printmessage "Initiating data load at target cluster on database ${dbname}."

    # Point to corresponding HQL file depending on whether external tables are to be included or not
    if [[ ${include_external_tables} == 'true' ]]; then
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
      if [[ ${post_load_repl_id} == ${source_latest_txid} ]] ; then
        printmessage "Database replication completed SUCCESSFULLY. Last transaction id at target is |${post_load_repl_id}|"
      elif [[ ${post_load_repl_id} == "NULL" ]] ; then
        printmessage "Database replication FAILED. No transactions have been applied in this run."
        # TODO - cleanup directories leftover during failed replication (if any) 
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
        
  # If source_latest_txid is anything but a proper number, 
  # it indicates a failure in geenerating the source dump. Exit.
  else
    printmessage "Unable to generate full dump for database ${dbname}. Exiting!."
    exit 1
  fi

# If the database at target cluster already has some transaction replayed, 
# trigger the source dump as an incremental dump.
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
 
  # dump_txid is set in the above function to the current transaction ID at source cluster for the database
  source_latest_txid=${dump_txid}
  printmessage "The current transaction ID at source is |${source_latest_txid}|"

  if [[ ${source_latest_txid} > 0 ]]; then
    printmessage "Database ${dbname} incremental dump has been generated at |${source_hdfs_prefix}${dump_path}|."
    # the calculation of txn_count below doesn't match with numEvents that show up in the 
    #txn_count=$((${source_latest_txid} - ${last_repl_id}))
    printmessage "Initiating REPL LOAD at destination cluster to replicate ${dbname} to transaction id |${source_latest_txid}|."
    
    # Point to corresponding HQL file depending on whether external tables are to be included or not
    if [[ ${include_external_tables} == 'true' ]]; then
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
      if [[ ${post_load_repl_id} == ${source_latest_txid} ]] ; then
        printmessage "Database replication completed SUCCESSFULLY. Last transaction id at target is |${post_load_repl_id}|"
      elif [[ ${post_load_repl_id} == ${last_repl_id} ]] ; then
        printmessage "Database replication FAILED. No transactions have been applied in this run."
        # TODO - cleanup directories leftover during failed replication (if any) 
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
    printmessage "Unable to generate incremental dump for database ${dbname}. Exiting!."
    exit 1
  fi

else
  printmessage "Invalid value for last replicated transaction id: ${last_repl_id}. Database dump failed"
  echo -e "See ${repl_log_file} for details. Exiting!"
  exit 1
fi
