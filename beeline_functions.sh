#!/bin/bash

retrieve_current_target_repl_id() {
# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target before replication
#
local out_file="${TMP_DIR}/repl_status_beeline.out"
beeline -u ${TARGET_JDBC_URL} ${BEELINE_OPTS} \
 -n ${BEELINE_USER} \
 --hivevar dbname=${DBNAME} \
 -f ${STATUS_HQL} \
 >${out_file} \
 2>>${REPL_LOG_FILE}
LAST_REPL_ID=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${out_file} )
}

retrieve_post_load_target_repl_id() {
# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target after replication
#
local out_file="${TMP_DIR}/post_load_repl_status_beeline.out"
beeline -u ${TARGET_JDBC_URL} ${BEELINE_OPTS} \
 -n ${BEELINE_USER} \
 --hivevar dbname=${DBNAME} \
 -f ${STATUS_HQL} \
 > ${out_file} \
 2>>${REPL_LOG_FILE} 
POST_LOAD_REPL_ID=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${out_file} )
}

gen_bootstrap_dump_source() {
# ----------------------------------------------------------------------------
# dump entire database at source hive instance for first time
#

# Point to corresponding HQL file depending on whether external tables are to be included or not
local hql_file=""
if [[ "${INCLUDE_EXTERNAL_TABLES}" == "true" ]]; then
  printmessage " INFO: Including external tables in full dump"
  hql_file=${EXT_BOOTSTRAP_HQL}
else 
  printmessage " INFO: Skipping external tables in full dump"
  hql_file=${BOOTSTRAP_HQL}
fi 
local out_file="${TMP_DIR}/repl_fulldump_beeline.out"

# Apply workaroud for the issue in page below for HDP 3.1.4 if flag is set in env.sh
# https://docs.cloudera.com/HDPDocuments/DLM1/DLM-1.5.1/administration/content/dlm_replchangemanager_error.html
if [[ "${INIT_REPL_CHANGE_MANAGER}" == "true" ]]; then
  local INIT_REPL_CHANGE_MANAGER_OUT_FILE="${TMP_DIR}/initReplChangeManager.out"
  beeline -u ${SOURCE_JDBC_URL} ${BEELINE_OPTS} \
    -n ${BEELINE_USER} \
    --hivevar dbname=${DBNAME} \
    -f ${INIT_REPL_CHANGE_MANAGER_HQL} \
    > ${INIT_REPL_CHANGE_MANAGER_OUT_FILE} \
    2>>${REPL_LOG_FILE}
fi

## Two bootstrap dumps should not run together. Hence adding a lock here.
local dump_lockfile=${RUN_DIR}/dump.lock

## Check if any bootstrap dump is running. If so exit.
if [[ -e "${dump_lockfile}" ]]; then
  printmessage " ERROR: Boostrap dump in progress by pid another database, exiting"
  exit 1
else
  ## Create the lockfile by printing the script's PID into it  and proceed
  echo $$ > ${dump_lockfile}
fi

beeline -u ${SOURCE_JDBC_URL} ${BEELINE_OPTS} \
 -n ${BEELINE_USER} \
 --hivevar dbname=${DBNAME} \
 -f ${hql_file} \
 > ${out_file} \
 2>>${REPL_LOG_FILE}

## If dump lock exists and is created by the current process, 
## remove the lock since dump is now complete
if [[ $(cat ${dump_lockfile}) == $$ ]]; then
  rm  ${dump_lockfile}
fi

# Extract dump path and transaction id from the output
DUMP_PATH=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${out_file})
DUMP_TXID=$(awk -F\| '(NR==4){gsub(/ /,"", $3);print $3}' ${out_file})

# Confirm database dump succeeded by verifying if location string returned 
# begins with configured location for replication dump.

if [[ "${DUMP_PATH}" != "${REPL_ROOT}"* ]]; then
  printmessage " ERROR: Could not generate database dump for ${DBNAME} at source.\n"
  return 0
else
  return 1
fi
}

gen_incremental_dump_source() {
# ----------------------------------------------------------------------------
# dump database at source hive instance from the last_repl_id at target
#
local hql_file=""
# dump generation command returns latest transaction id at source
if [[ "${INCLUDE_EXTERNAL_TABLES}" == "true" ]]; then
  printmessage " INFO: Including external tables in incremental dump"
  hql_file=${EXT_INC_DUMP_HQL}
else 
  printmessage " INFO: Skipping external tables in incremental dump"
  hql_file=${INC_DUMP_HQL}
fi 
local out_file="${TMP_DIR}/repl_incdump_beeline.out"
beeline -u ${SOURCE_JDBC_URL} ${BEELINE_OPTS} \
 -n ${BEELINE_USER} \
 --hivevar dbname=${DBNAME} \
 --hivevar last_repl_id=${LAST_REPL_ID} \
 -f ${hql_file} \
 > ${out_file} \
 2>>${REPL_LOG_FILE}

# Extract dump path and transaction id from the output
DUMP_PATH=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${out_file})
DUMP_TXID=$(awk -F\| '(NR==4){gsub(/ /,"", $3);print $3}' ${out_file})

# Confirm database dump succeeded by verifying if location string returned 
# begins with configured location for replication dump.

if [[ "${DUMP_PATH}" != "${REPL_ROOT}"* ]]
 then
  printmessage " ERROR: Could not generate database dump for ${DBNAME} at source.\n"
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
SOURCE_DUMP_PATH="${SOURCE_HDFS_PREFIX}${DUMP_PATH}"
local out_file="${TMP_DIR}/repl_load_beeline.out"
local hql_file=$1
local retry_counter=1
local retval=1

# Retry a failed incremental repl load upto $INCR_RERUN times
while [[ ${retry_counter} -le ${INCR_RERUN} ]]
do
  if [[ ${retry_counter} -gt 1 ]]
  then
      printmessage " INFO: Sleeping ${RERUN_SLEEP} seconds before retry"
      sleep ${RERUN_SLEEP}
      printmessage " INFO: Retrying load. Attempt number: ${retry_counter}"
  fi
  beeline -u ${TARGET_JDBC_URL} ${BEELINE_OPTS} \
    -n ${BEELINE_USER} \
    --hivevar dbname=${DBNAME} \
    --hivevar src_dump_path=${SOURCE_DUMP_PATH} \
    -f ${hql_file} \
    >${out_file} \
    2>>${REPL_LOG_FILE}
  retval=$?
  if [[ ${retval} -gt 0 ]]
  then
    printmessage " ERROR: REPL Load failed, return code is: ${retval}"
    printmessage " ERROR: Number of failed attempts: ${retry_counter}"
  else
    break
  fi
  retry_counter=$(( retry_counter + 1 ))
done
return ${retval}
}
