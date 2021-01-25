#!/bin/bash

retrieve_current_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target before replication
#
local OUT_FILE="${TMP_DIR}/repl_status_beeline.out"
beeline -u ${TARGET_JDBC_URL} ${BEELINE_OPTS} \
 -n ${BEELINE_USER} \
 --hivevar dbname=${DBNAME} \
 -f ${STATUS_HQL} \
 >${OUT_FILE} \
 2>>${REPL_LOG_FILE}

last_repl_id=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${OUT_FILE} )

}

retrieve_post_load_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target after replication
#
local OUT_FILE="${TMP_DIR}/post_load_repl_status_beeline.out"
beeline -u ${TARGET_JDBC_URL} ${BEELINE_OPTS} \
 -n ${BEELINE_USER} \
 --hivevar dbname=${DBNAME} \
 -f ${STATUS_HQL} \
 > ${OUT_FILE} \
 2>>${REPL_LOG_FILE} 
 
post_load_repl_id=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${OUT_FILE} )

}

gen_bootstrap_dump_source() {

# ----------------------------------------------------------------------------
# dump entire database at source hive instance for first time
#
local HQL_FILE=$1
local OUT_FILE="${TMP_DIR}/repl_fulldump_beeline.out"

if [[ ${INIT_REPL_CHANGE_MANAGER} == "true" ]]
then 
  # Apply workaroud for the issue in this page (HDP 3.1.4)
  # https://docs.cloudera.com/HDPDocuments/DLM1/DLM-1.5.1/administration/content/dlm_replchangemanager_error.html
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
if [ -e ${dump_lockfile} ]; then
  printmessage "Boostrap dump in progress by pid another database, exiting"
  exit 1
else
  ## Create the lockfile by printing the script's PID into it  and proceed
  echo $$ > ${dump_lockfile}
fi

beeline -u ${SOURCE_JDBC_URL} ${BEELINE_OPTS} \
 -n ${BEELINE_USER} \
 --hivevar dbname=${DBNAME} \
 -f ${HQL_FILE} \
 > ${OUT_FILE} \
 2>>${REPL_LOG_FILE}

## If dump lock exists and is created by the current process, 
## remove the lock since dump is now complete
if [[$(cat ${dump_lockfile}) == $$]]; then
  rm  ${dump_lockfile}
fi

# Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${OUT_FILE})
dump_txid=$(awk -F\| '(NR==4){gsub(/ /,"", $3);print $3}' ${OUT_FILE})

# Confirm database dump succeeded by verifying if location string returned 
# begins with configured location for replication dump.

if [[ ${dump_path} != ${REPL_ROOT}* ]]; then
  printmessage "Could not generate database dump for ${DBNAME} at source.\n"
  return 0
else
  return 1
fi
}

gen_incremental_dump_source() {
# ----------------------------------------------------------------------------
# dump database at source hive instance from the last_repl_id at target
#
local HQL_FILE=$1
local OUT_FILE="${TMP_DIR}/repl_incdump_beeline.out"
beeline -u ${SOURCE_JDBC_URL} ${BEELINE_OPTS} \
 -n ${BEELINE_USER} \
 --hivevar dbname=${DBNAME} \
 --hivevar last_repl_id=${last_repl_id} \
 -f ${HQL_FILE} \
 > ${OUT_FILE} \
 2>>${REPL_LOG_FILE}


# Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${OUT_FILE})
dump_txid=$(awk -F\| '(NR==4){gsub(/ /,"", $3);print $3}' ${OUT_FILE})

# Confirm database dump succeeded by verifying if location string returned 
# begins with configured location for replication dump.

if [[ ${dump_path} != ${repl_root}* ]]
 then
  printmessage "Could not generate database dump for ${DBNAME} at source.\n"
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
SOURCE_DUMP_PATH="${SOURCE_HDFS_PREFIX}${dump_path}"
local OUT_FILE="${TMP_DIR}/repl_load_beeline.out"
local LOAD_HQL=$1
local retry_counter=1
local retval=1

while [ ${retry_counter} -le $INCR_RERUN ]
do

  if [ ${retry_counter} -gt 1 ]
  then
      printmessage "Retrying load. Attempt number: ${retry_counter}"
  fi
  
  beeline -u ${TARGET_JDBC_URL} ${BEELINE_OPTS} \
    -n ${BEELINE_USER} \
    --hivevar dbname=${DBNAME} \
    --hivevar src_dump_path=${SOURCE_DUMP_PATH} \
    -f ${LOAD_HQL} \
    >${OUT_FILE} \
    2>>${REPL_LOG_FILE}

  retval=$?
  if [ ${retval} -gt 0 ]
  then
    printmessage "REPL Load failed, return code is: ${retval}"
    printmessage "Number of failed attempts: ${retry_counter}"
  else
    break
  fi
  retry_counter=$[${retry_counter}+1]
done

return ${retval}

}
