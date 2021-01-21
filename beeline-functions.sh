#!/bin/bash

retrieve_current_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target before replication
#
local out_file="${TMP_DIR}/repl_status_beeline.out"
beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${STATUS_HQL} \
 >${out_file} \
 2>>${repl_log_file}

last_repl_id=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${out_file} )

}

retrieve_post_load_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target after replication
#
local out_file="${TMP_DIR}/post_load_repl_status_beeline.out"
beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${STATUS_HQL} \
 > ${out_file} \
 2>>${repl_log_file} 
 
post_load_repl_id=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${out_file} )

}

gen_bootstrap_dump_source() {

# ----------------------------------------------------------------------------
# dump entire database at source hive instance for first time
#
local HQL_FILE=$1
local out_file="${TMP_DIR}/repl_fulldump_beeline.out"

if [[ ${initReplChangeManager} == "true" ]]
then 
  # Apply workaroud for the issue in this page (HDP 3.1.4)
  # https://docs.cloudera.com/HDPDocuments/DLM1/DLM-1.5.1/administration/content/dlm_replchangemanager_error.html
  local initReplChangeManager_out_file="${TMP_DIR}/initReplChangeManager.out"

  beeline -u ${source_jdbc_url} ${beeline_opts} \
    -n ${beeline_user} \
    --hivevar dbname=${dbname} \
    -f ${INITREPLCHANGEMANAGER_HQL} \
    > ${initReplChangeManager_out_file} \
    2>>${repl_log_file}
fi

beeline -u ${source_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${HQL_FILE} \
 > ${out_file} \
 2>>${repl_log_file}

# Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${out_file})
dump_txid=$(awk -F\| '(NR==4){gsub(/ /,"", $3);print $3}' ${out_file})

# Confirm database dump succeeded by verifying if location string returned 
# begins with configured location for replication dump.

if [[ ${dump_path} != ${repl_root}* ]]; then
  printmessage "Could not generate database dump for ${dbname} at source.\n"
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
local out_file="${TMP_DIR}/repl_incdump_beeline.out"
beeline -u ${source_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 --hivevar last_repl_id=${last_repl_id} \
 -f ${HQL_FILE} \
 > ${out_file} \
 2>>${repl_log_file}


# Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==4){gsub(/ /,"", $2);print $2}' ${out_file})
dump_txid=$(awk -F\| '(NR==4){gsub(/ /,"", $3);print $3}' ${out_file})

# Confirm database dump succeeded by verifying if location string returned 
# begins with configured location for replication dump.

if [[ ${dump_path} != ${repl_root}* ]]
 then
  printmessage "Could not generate database dump for ${dbname} at source.\n"
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
local out_file="${TMP_DIR}/repl_load_beeline.out"
local LOAD_HQL=$1
local retry_counter=1
local retval=1

while [ ${retry_counter} -le $INCR_RERUN ]
do

  if [ ${retry_counter} -gt 1 ]
  then
      printmessage "Retrying load. Attempt number: ${retry_counter}"
  fi
  
  beeline -u ${target_jdbc_url} ${beeline_opts} \
    -n ${beeline_user} \
    --hivevar dbname=${dbname} \
    --hivevar src_dump_path=${src_dump_path} \
    -f ${LOAD_HQL} \
    >${out_file} \
    2>>${repl_log_file}

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
