#!/bin/bash

printmessage() {
# ----------------------------------------------------------------------------
# Function to add timestamps to messages and write to log file
#
# Globals modified by function:
#   None
# Arguments:
#   Log message
# Outputs:
#   Logs to REPL_LOG_FILE
# Returns:
#   No specific value
# ----------------------------------------------------------------------------
  local now=$(date +%Y-%m-%d\ %H:%M:%S.$(( $(date +%-N) / 1000000 )))
  local message="$now $*"
  echo -e ${message} | tee -a ${REPL_LOG_FILE}
}

trap_log_int() {
# ----------------------------------------------------------------------------
# Function to identify if a Ctrl-C has been issued against the script run.
#
# Globals modified by function:
#   None
# Arguments:
#   None
# Outputs:
#   Logs to REPL_LOG_FILE
#   Removes any lock file created by current script instance
# Returns:
#   No specific value
# ----------------------------------------------------------------------------
  printmessage " ERROR: Ctrl-C attempted. Aborting!"
  if [[ "${APPLY_DB_LOCK}" == "true" ]]; then
    # Removing lock file upon completion of run
    local lock_file="${RUN_DIR}/$1"
    # A second script checking the lock and exiting should not remove the lock
    # of the first instance which is running. Henc adding a pid check
    if [[ $(cat "${lock_file}") == $$ ]]; then
      printmessage " INFO: Removing lock file ${lock_file}"
      rm  ${lock_file}
    fi
  fi

  ## If dump lock exists and is created by the current process, 
  ## remove the lock as we are aborting!
  local dump_lockfile=${RUN_DIR}/dump.lock
  if [[ -e "${dump_lockfile}" ]]; then
    if [[$(cat "${dump_lockfile}") == $$ ]]; then
      printmessage " INFO: Removing Dump lock file ${lock_file}"
      rm  ${dump_lockfile}
    fi
  fi
  printmessage " INFO: Lock files removed"
}

trap_log_exit() {
# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target after replication
#
# Globals modified by function:
#   None
# Arguments:
#   DB level lock file name
# Outputs:
#   Logs to REPL_LOG_FILE
#   Removes any lock file created by current script instance
# Returns:
#   No specific value
# ----------------------------------------------------------------------------
  # Removing unnecessary warnings from SLF4J library, 
  sed -i '/^SLF4J:/d' ${REPL_LOG_FILE}
  # Removing some empty lines generated by beeline
  sed -i '/^$/d' ${REPL_LOG_FILE}
  # Removing lock file upon completion of run
  if [[ "${APPLY_DB_LOCK}" == "true" ]]; then
    local lock_file="${RUN_DIR}/$1"
    # A second script checking the lock and exiting should not remove the lock
    # of the first instance which is running. Henc adding a pid check
    if [[ -e "${lock_file}" ]]; then
      if [[ $(cat "${lock_file}") == $$ ]]; then
        printmessage " INFO: Removing lock file ${lock_file}"
        rm  ${lock_file}
      fi
    fi
  fi
  
  ## Remove dump lock file if somehow it wasn't removed in the 
  ## correct stage inside gen_bootstrap_dump_source function
  local dump_lockfile=${RUN_DIR}/dump.lock
  ## If dump lock exists and is created by the current process, 
  ## remove the lock since dump is now complete
  if [[ -e "${dump_lockfile}" ]]; then
    if [[ $(cat "${dump_lockfile}") == $$ ]]; then
      printmessage " INFO: Removing Dump lock file ${dump_lockfile}"
      rm  ${dump_lockfile}
    fi
  fi
  
  printmessage " INFO: Lock files removed"
  local duration=$SECONDS
  printmessage " INFO: Script run took $(($duration / 60)) minutes and $(($duration % 60)) seconds "

  if [[ "${HDFS_UPLOAD}" == "true" ]]; then
    if [[ "${HDFS_UPLOAD_DIR}" != "" ]]; then
      upload_logs_to_hdfs
    else
      printmessage " WARN: No path specified for HDFS upload."
      printmessage " WARN: Will skip log upload to HDFS."
    fi
  fi
  
}

upload_logs_to_hdfs() {

# ----------------------------------------------------------------------------
# Upload the $REPL_LOG_FILE to an HDFS location for remote access
#
# Globals modified by function:
#   None
# Arguments:
#   None
# Outputs:
#   Logs to REPL_LOG_FILE
#   Log files copied to HDFS location
# Returns:
#   No specific value
# ----------------------------------------------------------------------------

  # Check if upload directory exists in HDFS
  hdfs dfs -test -d ${HDFS_UPLOAD_DIR}
  local dirtest_retval=$?
  if [[ "${dirtest_retval}" -eq 0 ]]; then
    # if path exists will attempt log upload.TODO: Check perms before upload.
    printmessage " INFO: Uploading replication log to HDFS Upload directory."
    hdfs dfs -put ${REPL_LOG_FILE} ${HDFS_UPLOAD_DIR} 2>&1
    local upload_retval=$?
    if [[ "${upload_retval}" -eq 0 ]]; then
      echo " INFO: Uploaded replication log to HDFS Upload directory."
    else
      echo " ERROR: Replication log upload to HDFS Upload directory failed."
    fi
  else
    printmessage " WARN: Upload path ${HDFS_UPLOAD_DIR} does not exist in HDFS. "
    printmessage " WARN: Will skip log upload to HDFS."
  fi

}

check_instance_lock() {
# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target after replication
#
# Globals modified by function:
#   None
# Arguments:
#   lock file name
# Outputs:
#   Logs to REPL_LOG_FILE
#   Writes lock file for current script instance
# Returns:
#   No specific value
# ----------------------------------------------------------------------------

local lock_file="${RUN_DIR}/$1"
## If the lock file exists
if [[ -e "${lock_file}" ]]; then
    ## Check if the PID in the lockfile is a running instance
    ## of ${SCRIPT_NAME} to guard against failed runs
    if ps $(cat "${lock_file}") | grep "${SCRIPT_NAME}" >/dev/null; then
        printmessage " ERROR: Script ${SCRIPT_NAME} is already running for ${DBNAME}, exiting"
        exit 1
    else
        printmessage " ERROR: Lockfile ${lock_file} contains a stale PID."
        printmessage " ERROR: A previous replication run for ${DBNAME} may have failed halfway."
        printmessage " ERROR: Please confirm if the previous process exited, then delete the lock file: ${lock_file} before proceeding."
        exit 1
    fi
fi
## Create the lockfile by printing the script's PID into it
echo $$ > "${lock_file}"
}

check_db_validity() {
# ----------------------------------------------------------------------------
# Validate dbname provided against list of valid names specified in env.sh
#
# Globals modified by function:
#   None
# Arguments:
#   database name
# Outputs:
#   Logs to console
# Returns:
#   $dbvalidity value
# ----------------------------------------------------------------------------
local dbname=$1
local dbvalidity=1
for db in ${DBLIST}; do
    if [[ "${dbname}" == "${db}" ]]; then
      echo "INFO: Database name ${dbname} validated successfully."
      dbvalidity=0
    fi
done
return ${dbvalidity}
}

script_usage() {

# ----------------------------------------------------------------------------
# Prints help message
#
# Globals modified by function:
#   None
# Arguments:
#   database name
# Outputs:
#   Logs to console
# Returns:
#   No specific value
# ----------------------------------------------------------------------------
  echo -e "Usage : ${BASENAME} <database-name> \n"
  echo -e "**  It is recommended to run this script at the target cluster, but it should work in either cluster.\n" 
  echo -e "**  The database name is a required argument and is validated against the dblist variable in env.sh. \n"
}
