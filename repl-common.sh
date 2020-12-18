#!/bin/bash
source ./env.sh

printmessage() {
  local now=`date +%Y-%m-%d\ %H:%M:%S.$(( $(date +%-N) / 1000000 ))`
  local message="$now $*"
  echo -e ${message} | tee -a ${repl_log_file}
}

trap_log_int() {
  printmessage "Ctrl-C attempted. Aborting!"
}

trap_log_exit() {
  sed -i '/^SLF4J:/d' ${repl_log_file}
  sed -i '/^$/d' ${repl_log_file}
}

script_usage() {

  echo -e "Usage : ${BASENAME} <database-name> [DEBUG] \n"
  echo -e "**  It is recommended to run this script at the target cluster, but it should work in either cluster.\n" 
  echo -e "**  The database name is a required argument and is validated against the dblist variable in env.sh. \n"
  echo -e "**  Use the string DEBUG as the last argument for verbose output.\n"
}
