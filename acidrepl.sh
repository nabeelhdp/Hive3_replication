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

script_usage()
{
  echo -e "Usage : ${BASENAME} [target-database-name] [debug] \n"
  echo -e "**  This script is to be run on your target cluster. When run without \n"
  echo -e "**  database name as argument, the target database name is considered \n"
  echo -e "**  same as source defined in env.sh. \n"
  echo -e "**  Any database name passed is validated against dblist variable in env.sh. \n"
  echo -e "**  DEBUG is optional. When set, DEBUG provides all beeline outputs in log.\n"
}

BOOTSTRAP_HQL="${HQL_DIR}/repldump.hql"
INC_DUMP_HQL="${HQL_DIR}/replbootstrap.hql"
LOAD_HQL="${HQL_DIR}/replload.hql"
STATUS_HQL="${HQL_DIR}/replstatus.hql"

retrieve_current_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target
#
repl_status_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${targetdbname} \
 -f ${STATUS_HQL} >beeline.op 2>>${repl_log_file} )

 if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "Beeline output \n "
   cat beeline.op >> ${repl_log_file}
 fi

last_repl_id=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' beeline.op )

echo ${last_repl_id}

}

gen_bootstrap_dump_source() {

# ----------------------------------------------------------------------------
# dump entire database at source hive instance for first time
#
repl_dump_retval=$(beeline -u ${source_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${BOOTSTRAP_HQL} 1>beeline.op 2>>${repl_log_file})

if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "Beeline output \n"
   cat beeline.op >> ${repl_log_file}
fi

 # Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' beeline.op)
dump_txid=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $3}' beeline.op)

 # Confirm database dump succeeded

if [[ ${dump_path} != ${repl_root}* ]]
 then
  printmessage "Could not generate database dump for ${dbname} at source.\n"
  # If debug is enabled, the output would already be written earlier. So
  # skipping a write of output into log a second time.
  if [[ "${loglevel}" == "INFO" ]]; then
     cat beeline.op >> ${repl_log_file}
  fi
  return 0
else
  return ${dump_txid}
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
 -f ${INC_DUMP_HQL} 1>beeline.op  2>>${repl_log_file})

if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "Beeline output \n"
   cat beeline.op >> ${repl_log_file}
fi

# Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' beeline.op)
dump_txid=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $3}' beeline.op)

# Confirm database dump succeeded

if [[ ${dump_path} != /app* ]]
 then
 echo -e "Could not dump database \n${repl_dump_output}"
fi
}

replay_dump_at_target(){
# ----------------------------------------------------------------------------
# Load database at target from hdfs location in source
#

# Add prefix for source cluster to dump directory when running at target cluster
src_dump_path="${source_hdfs_prefix}${dump_path}"

repl_load_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${targetdbname} \
 --hivevar src_dump_path=${src_dump_path} \
 -f ${LOAD_HQL} 1>beeline.op 2>>${repl_log_file})

if [[ "${loglevel}" == "$DEBUG" ]]; then
  printmessage "Beeline output \n${repl_load_output}"
fi

# Confirm database load succeeded
#
if [[ ${dump_path} != ${repl_root}* ]]
 then
  printmessage "Could not generate database dump for ${dbname} at source.\n"
  # If debug is enabled, the output would already be written earlier. So
  # skipping a write of output into log a second time.
  if [[ "${loglevel}" == "INFO" ]]; then
     cat beeline.op >> ${repl_log_file}
  fi
  return 0
else
  return ${dump_txid}
fi

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
last_repl_id=$(retrieve_current_target_repl_id)

if [[ ${last_repl_id} == "NULL" ]] ; then
  printmessage "No replication id detected at target. Full data dump dump needs to be initiated."
  read  -n 1 -p "Continue with full dump ? Y:N" fulldumpconfirmation
  if [[ ${fulldumpconfirmation} == "Y" ]]; then
    printmessage "Database ${dbname} is being synced for the first time. Initiating full dump."
    # dump generation command returns latest transaction id at source
    source_latest_txid=$(gen_bootstrap_dump_source)
    if [[ ${source_latest_txid} > 0 ]]; then
      printmessage "Database ${dbname} full dump has been generated at ${dump_path}."
      printmessage "The current transaction ID at source is ${source_latest_txid}"
      printmessage "There are ${source_latest_txid} transactions to be synced in this run."
      replay_dump_at_target
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
  source_latest_txid=$(gen_incremental_dump_source)
  if [[ ${source_latest_txid} > 0 ]]; then
    printmessage "Database ${dbname} incremental dump has been generated at ${dump_path}."
    printmessage "The current transaction ID at source is ${source_latest_txid}"
    txn_count=$((${source_latest_txid} - ${last_repl_id}))
    printmessage "There are ${txn_count} transactions to be synced in this run."
    replay_dump_at_target
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
