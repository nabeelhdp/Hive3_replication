#!/bin/bash

###################################################################
# Script Name : acid-repl.sh
# Description : Replicate Hive databases with managed tables
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

trap trap_log_int INT
trap trap_log_exit EXIT

script_usage() {

  echo -e "Usage : ${BASENAME} <database-name> [DEBUG] \n"
  echo -e "**  It is recommended to run this script at the target cluster, but it should work in either cluster.\n" 
  echo -e "**  The database name is a required argument and is validated against the dblist variable in env.sh. \n"
  echo -e "**  Use the string DEBUG as the last argument for verbose output.\n"
}

last_repl_id=""
post_load_repl_id=""
dump_path=""
dump_txid=""

retrieve_current_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target
#
repl_status_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${STATUS_HQL} >repl_status_beeline.out 2>>${repl_log_file} )

 if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "REPL STATUS Beeline output : "
   cat repl_status_beeline.out >> ${repl_log_file}
 fi

last_repl_id=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' repl_status_beeline.out )

[[ ${last_repl_id} =~ ${re} ]] && return 0
return 1

}

retrieve_post_load_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target
#
post_load_repl_status_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${STATUS_HQL} >post_load_repl_status_beeline.out 2>>${repl_log_file} )

 if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "REPL STATUS Beeline output : "
   cat post_load_repl_status_beeline.out >> ${repl_log_file}
 fi

post_load_repl_id=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' post_load_repl_status_beeline.out )

[[ ${post_load_repl_id} =~ ${re} ]] && return 0
return 1

}

gen_bootstrap_dump_source() {

# ----------------------------------------------------------------------------
# dump entire database at source hive instance for first time
#
repl_dump_retval=$(beeline -u ${source_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${BOOTSTRAP_HQL} >repl_fulldump_beeline.out  2>>${repl_log_file})

if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "Beeline output :"
   cat repl_fulldump_beeline.out >> ${repl_log_file}
fi

 # Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' repl_fulldump_beeline.out)
dump_txid=$(awk -F\| '(NR==2){gsub(/ /,"", $3);print $3}' repl_fulldump_beeline.out)

 # Confirm database dump succeeded

if [[ ${dump_path} != ${repl_root}* ]]; then
  printmessage "Could not generate database dump for ${dbname} at source.\n"
  # If debug is enabled, the output would already be written earlier. So
  # skipping a write of output into log a second time.
  if [[ "${loglevel}" == "INFO" ]]; then
     cat repl_fulldump_beeline.out >> ${repl_log_file}
  fi
  return 0
else
  return 1
fi
}

gen_incremental_dump_source() {
# ----------------------------------------------------------------------------
# dump database at source hive instance from the last_repl_id at target
#
repl_dump_retval=$(beeline -u ${source_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 --hivevar last_repl_id=${last_repl_id} \
 -f ${INC_DUMP_HQL} >repl_incdump_beeline.out  2>>${repl_log_file})

# Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' repl_incdump_beeline.out)
dump_txid=$(awk -F\| '(NR==2){gsub(/ /,"", $3);print $3}' repl_incdump_beeline.out)

if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "REPL DUMP Beeline output :"
   cat repl_incdump_beeline.out >> ${repl_log_file}
fi

# Confirm database dump succeeded

if [[ ${dump_path} != ${repl_root}* ]]
 then
  printmessage "Could not generate database dump for ${dbname} at source.\n"
  # If debug is enabled, the output would already be written earlier. So
  # skipping a write of output into log a second time.
  if [[ "${loglevel}" == "INFO" ]]; then
     cat repl_incdump_beeline.out  >> ${repl_log_file}
  fi
  return 0
else
  return 1
fi

}

replay_dump_at_target(){
# ----------------------------------------------------------------------------
# Load database at target from hdfs location in source
#

# Add prefix for source cluster to dump directory when running at target cluster
src_dump_path="${source_hdfs_prefix}${dump_path}"
local repl_load_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 --hivevar src_dump_path=${src_dump_path} \
 -f ${LOAD_HQL} >repl_load_beeline.out 2>>${repl_log_file})

if [[ "${loglevel}" == "$DEBUG" ]]; then
  printmessage "REPL LOAD Beeline output :"
  cat repl_load_beeline.out >> ${repl_log_file}
fi

# Confirm database load succeeded
#
# return 0 returns to where the function was called.  $? contains 0 (success).
# return 1 returns to where the function was called.  $? contains 1 (failure).

grep "INFO  : OK" repl_load_beeline.out  && return 0
return 1
}

################ MAIN BEGINS HERE #########################

# Argument count should be either 1 or 2
[[ "$#" > 2 ]] || [[ "$#" < 1 ]]; then
  script_usage
fi

# If argument count is 1 or 2, the first argument is the db name 
dbname=$1
# By default, loglevel is INFO
loglevel="INFO"

# If second argument exists, it should be DEBUG, else ignore
if [[ "$2" == "DEBUG" ]]; then
  loglevel="DEBUG" 
  printmessage "Enabling DEBUG output"
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

# Retrieve the current state of replication in the target cluster.
retrieve_current_target_repl_id

if [[ ${last_repl_id} == "NULL" ]] ; then
  printmessage "No replication id detected at target. Full data dump dump needs to be initiated."
  # Remove the comment from the line below and comment out the line after to let the script run non-interactively
  fulldumpconfirmation="Y" 
  # read  -n 1 -t 30 -rep $'Continue with full dump ? Y:N \n' fulldumpconfirmation
  if [[ ${fulldumpconfirmation} == "Y" ]]; then
    printmessage "Database ${dbname} is being synced for the first time. Initiating full dump."
    # dump generation command returns latest transaction id at source
    gen_bootstrap_dump_source
    source_latest_txid=${dump_txid}
    printmessage "Source transaction id: |${source_latest_txid}|"

    if [[ ${source_latest_txid} > 0 ]]; then
      printmessage "Database ${dbname} full dump has been generated at |${source_hdfs_prefix}${dump_path}|."
      printmessage "The current transaction ID at source is |${source_latest_txid}|"
      printmessage "There are ${source_latest_txid} transactions to be synced in this run."
      printmessage "Initiating data load at target cluster on database ${dbname}."
      replay_dump_at_target && printmessage "Data load at target cluster failed" && echo -e "See ${repl_log_file} for details. Exiting!" && exit 1
      retrieve_post_load_target_repl_id
      if [[ ${post_load_repl_id} == ${source_latest_txid} ]] ; then
        printmessage "Database synchronized successfully. Last transaction id at target is |${post_load_repl_id}|"
      else
        printmessage "Invalid latest transaction id returned from Source : |${source_latest_txid}|"
        printmessage "Unable to generate incremental dump for database ${dbname}. Exiting!." && exit 1
      fi

    else
      printmessage "Unable to generate full dump for database ${dbname}. Exiting!."
      exit 1
    fi

  else
    echo "Aborting replication attempt. Exiting!"
    exit 1
  fi

elif [[ ${last_repl_id} =~ ${re} ]] ; then
  printmessage "Database ${dbname} transaction ID at target is currently ${last_repl_id}"
  gen_incremental_dump_source
  source_latest_txid=${dump_txid}
  printmessage "Source transaction id: |${source_latest_txid}|"

  if [[ ${source_latest_txid} > 0 ]]; then
    printmessage "Database ${dbname} incremental dump has been generated at |${source_hdfs_prefix}${dump_path}|."
    printmessage "The current transaction ID at source is |${source_latest_txid}|"
    txn_count=$((${source_latest_txid} - ${last_repl_id}))
    printmessage "There are ${txn_count} transactions to be synced in this run."
    replay_dump_at_target && printmessage "Data load at target cluster failed" && echo -e "See ${repl_log_file} for details. Exiting!" && exit 1
    retrieve_post_load_target_repl_id
    if [[ ${post_load_repl_id} == ${source_latest_txid} ]] ; then
      printmessage "Database replication completed SUCCESSFULLY. Latest transaction id at target is |${post_load_repl_id}|"
    else
      printmessage "Database replication FAILED ! Post Load repl id: |${post_load_repl_id}|"
      printmessage "Source repl id: |${source_latest_txid}|"
      exit 1
    fi
  else
    printmessage "Invalid latest transaction id returned from Source : |${source_latest_txid}|"
    printmessage "Unable to generate incremental dump for database ${dbname}. Exiting!."
    exit 1
  fi

else
  printmessage "Invalid value for last replicated transaction id: ${last_repl_id}. Database dump failed"
  echo -e "See ${repl_log_file} for details. Exiting!"
  exit 1
fi

