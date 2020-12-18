#!/bin/bash

retrieve_current_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target
#
out_file='${TMP_DIR}/repl_status_beeline.out'
repl_status_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${STATUS_HQL} \
 >${out_file} \
 2>>${repl_log_file} )

 if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "REPL STATUS Beeline output : "
   cat ${out_file} >> ${repl_log_file}
 fi

last_repl_id=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' ${out_file} )

[[ ${last_repl_id} =~ ${re} ]] && return 0
return 1

}

retrieve_post_load_target_repl_id() {

# ----------------------------------------------------------------------------
# Retrieve current last_repl_id for database at target
#
out_file='${TMP_DIR}/post_load_repl_status_beeline.out'
post_load_repl_status_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${STATUS_HQL} \
 > ${out_file} \
 2>>${repl_log_file} )

 if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "REPL STATUS Beeline output : "
   cat ${out_file} >> ${repl_log_file}
 fi

post_load_repl_id=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' ${out_file} )

[[ ${post_load_repl_id} =~ ${re} ]] && return 0
return 1

}

gen_bootstrap_dump_source() {

# ----------------------------------------------------------------------------
# dump entire database at source hive instance for first time
#
HQL_FILE=$1
out_file='${TMP_DIR}/repl_fulldump_beeline.out'
repl_dump_retval=$(beeline -u ${source_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 -f ${HQL_FILE} \
 > ${out_file} \
 2>>${repl_log_file})

if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "Beeline output :"
   cat ${out_file} >> ${repl_log_file}
fi

 # Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' ${out_file})
dump_txid=$(awk -F\| '(NR==2){gsub(/ /,"", $3);print $3}' ${out_file})

 # Confirm database dump succeeded

if [[ ${dump_path} != ${repl_root}* ]]; then
  printmessage "Could not generate database dump for ${dbname} at source.\n"
  # If debug is enabled, the output would already be written earlier. So
  # skipping a write of output into log a second time.
  if [[ "${loglevel}" == "INFO" ]]; then
    cat ${out_file} >> ${repl_log_file}
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
HQL_FILE=$1
out_file='${TMP_DIR}/repl_incdump_beeline.out'
repl_dump_retval=$(beeline -u ${source_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 --hivevar last_repl_id=${last_repl_id} \
 -f ${HQL_FILE} \
 > ${out_file} \
 2>>${repl_log_file})

# Extract dump path and transaction id from the output
dump_path=$(awk -F\| '(NR==2){gsub(/ /,"", $2);print $2}' ${out_file})
dump_txid=$(awk -F\| '(NR==2){gsub(/ /,"", $3);print $3}' ${out_file})

if [[ "${loglevel}" == "DEBUG" ]]; then
   printmessage "REPL DUMP Beeline output :"
   cat ${out_file} >> ${repl_log_file}
fi

# Confirm database dump succeeded

if [[ ${dump_path} != ${repl_root}* ]]
 then
  printmessage "Could not generate database dump for ${dbname} at source.\n"
  # If debug is enabled, the output would already be written earlier. So
  # skipping a write of output into log a second time.
  if [[ "${loglevel}" == "INFO" ]]; then
    cat ${out_file} >> ${repl_log_file}
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
out_file='${TMP_DIR}/repl_load_beeline.out'
LOAD_HQL=$1

local repl_load_retval=$(beeline -u ${target_jdbc_url} ${beeline_opts} \
 -n ${beeline_user} \
 --hivevar dbname=${dbname} \
 --hivevar src_dump_path=${src_dump_path} \
 -f ${LOAD_HQL} \
  >${out_file} \
  2>>${repl_log_file})

if [[ "${loglevel}" == "$DEBUG" ]]; then
  printmessage "REPL LOAD Beeline output :"
  cat ${out_file} >> ${repl_log_file}
fi

# Confirm database load succeeded
#
# return 0 returns to where the function was called.  $? contains 0 (success).
# return 1 returns to where the function was called.  $? contains 1 (failure).

grep "INFO  : OK" ${out_file}  && return 0
return 1
}
