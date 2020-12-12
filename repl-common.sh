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
