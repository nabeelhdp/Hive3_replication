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

trap trap_log INT

script_usage() {

  echo -e "Usage : ${BASENAME} [target-database-name] [debug] \n"
  echo -e "**  This script is to be run on your target cluster. When run without \n"
  echo -e "**  database name as argument, the target database name is considered \n"
  echo -e "**  same as source defined in env.sh. \n"
  echo -e "**  Any database name passed is validated against dblist variable in env.sh. \n"
  echo -e "**  DEBUG is optional. When set, DEBUG provides all beeline outputs in log.\n"
}

last_repl_id=""
dump_path=""
dump_txid=""

retrieve_current_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target
#
repl_status_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${targetdbname} \
 -f ${STATUS_HQL} >repl_status_beeline.out 2>>${repl_log_file} )

 if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "REPL STATUS Beeline output : "
   cat repl_status_beeline.out >> ${repl_log_file}
 fi

last_repl_id=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' repl_status_beeline.out )

[[ ${last_repl_id} =~ ${re} ]] && return 0
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
dump_txid=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $3}' repl_fulldump_beeline.out)

 # Confirm database dump succeeded

if [[ ${dump_path} != ${repl_root}* ]]
 then
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
dump_txid=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $3}' repl_incdump_beeline.out)

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
echo $src_dump_path
repl_load_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${targetdbname} \
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

################ FLOW BEGINS HERE #########################

if [[ $1 == "help" ]]; then
  script_usage
  exit 1
fi

# Target DB Name can be overriden when passed as argument to script
if [[ "$1" != "" ]]; then
   targetdbname=$1
fi

# Validate dbname provided against list of valid names specified in env.sh
dbvalidity="0"
for val in $dblist; do
    if [[ $val == ${targetdbname} ]]; then
      dbvalidity="1"
    fi
done

if [[ ${dbvalidity} == "0" ]]; then
  printmessage "Invalid target database name specified. Falling back to source name."
  targetdbname=${dbname}
fi

# Set debug if passed as first or second argument to script
loglevel="INFO"
shopt -s nocasematch;
[[ "$2" == "DEBUG" ]] || [[ "$1" == "debug" ]] && loglevel="DEBUG" && printmessage "Enabling DEBUG output"
shopt -u nocasematch;

printmessage "==================================================================="
printmessage "Initiating run to replicate ${dbname} to ${targetdbname} "
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
  read  -n 1 -p "Continue with full dump ? Y:N \n" fulldumpconfirmation

  if [[ ${fulldumpconfirmation} == "Y" ]]; then
    printmessage "Database ${dbname} is being synced for the first time. Initiating full dump."
    # dump generation command returns latest transaction id at source
    gen_bootstrap_dump_source
    source_latest_txid=${dump_txid}

    if [[ ${source_latest_txid} > 0 ]]; then
      printmessage "Database ${dbname} full dump has been generated at ${dump_path}."
      printmessage "The current transaction ID at source is ${source_latest_txid}"
      printmessage "There are ${source_latest_txid} transactions to be synced in this run."
      printmessage "Initiating data load at target cluster on database ${targetdbname}."
      replay_dump_at_target || printmessage "Data load at target cluster failed" && echo -e "See ${repl_log_file} for details. Exiting!"
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

  if [[ ${source_latest_txid} > 0 ]]; then
    printmessage "Database ${dbname} incremental dump has been generated at ${dump_path}."
    printmessage "The current transaction ID at source is ${source_latest_txid}"
    txn_count=$((${source_latest_txid} - ${last_repl_id}))
    printmessage "There are ${txn_count} transactions to be synced in this run."
    replay_dump_at_target || printmessage "Data load at target cluster failed" && echo -e "See ${repl_log_file} for details. Exiting!"
  else
    printmessage "Invalid latest transaction id returned from Source : ${source_latest_txid}"
    printmessage "Unable to generate incremental dump for database ${dbname}. Exiting!."
    exit 1
  fi

else
  printmessage "Invalid value for last replicated transaction id: ${last_repl_id}. Database dump failed"
  echo -e "See ${repl_log_file} for details. Exiting!"
  exit 1
fi

