#!/bin/bash

printmessage() {

# ----------------------------------------------------------------------------
# Function to add timestamps to messages and write to log file
#
  local now=`date +%Y-%m-%d\ %H:%M:%S.$(( $(date +%-N) / 1000000 ))`
  local message="$now $*"
  echo -e ${message} | tee -a ${repl_log_file}
}

trap_log_int() {

# ----------------------------------------------------------------------------
# Function to identify if a Ctrl-C has been issued against the script run.
#
  printmessage "Ctrl-C attempted. Aborting!"
  local lock_file="${RUN_DIR}/$1"

  # Removing lock file upon completion of run
  # A second script checking the lock and exiting should not remove the lock
  # of the first instance which is running. Henc adding a pid check
  if [[$(cat ${lock_file}) == $$]]; then
    rm  ${lock_file}
  fi

}

trap_log_exit() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target after replication
#

  local lock_file="${RUN_DIR}/$1"

  # Removing unnecessary warnings from SLF4J library, 
  sed -i '/^SLF4J:/d' ${repl_log_file}
  
  # Removing some empty lines generated by beeline
  sed -i '/^$/d' ${repl_log_file}
  
  # Removing lock file upon completion of run
  # A second script checking the lock and exiting should not remove the lock
  # of the first instance which is running. Henc adding a pid check
  if [[$(cat ${lock_file}) == $$]]; then
    rm  ${lock_file}
  fi
  
  # Check if upload directory exists in HDFS
  hdfs dfs -test -d ${hdfs_upload_dir}
  local dirtest_retval=$?
  if [[ ${dirtest_retval} -eq 0 ]]; then
    # if path exists will attempt log upload.TODO: Check perms before upload.
    printmessage "Uploading replication log to HDFS Upload directory."
    hdfs dfs -put ${repl_log_file} ${hdfs_upload_dir}
    local upload_retval=$?
    if [[ ${upload_retval} -eq 0 ]]; then
      echo "Uploaded replication log to HDFS Upload directory."
    else
      echo "Replication log upload to HDFS Upload directory failed."
    fi
  else
    printmessage "Upload path ${hdfs_upload_dir} does not exist in HDFS. "
    printmessage "Will skip log upload to HDFS."
  fi

}

check_instance_lock() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target after replication
#

local lock_file="${RUN_DIR}/$1"

## If the lock file exists
if [ -e ${lock_file} ]; then

    ## Check if the PID in the lockfile is a running instance
    ## of ${script_name} to guard against failed runs
    if ps $(cat ${lock_file}) | grep ${script_name} >/dev/null; then
        printmessage "Script ${script_name} is already running for ${dbname}, exiting"
        exit 1
    else
        printmessage "Lockfile ${lock_file} contains a stale PID."
        printmessage "A previous replication run may still be running for ${dbname}."
        printmessage "Please confirm if the previous process exited, then delete the lock file:"
        printmessage "${lock_file} before proceeding."
        exit 1
    fi
fi
## Create the lockfile by printing the script's PID into it
echo $$ > ${lock_file}

}

script_usage() {
  echo -e "Usage : ${BASENAME} <database-name> \n"
  echo -e "**  It is recommended to run this script at the target cluster, but it should work in either cluster.\n" 
  echo -e "**  The database name is a required argument and is validated against the dblist variable in env.sh. \n"
}
