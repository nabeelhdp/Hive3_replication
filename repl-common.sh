#!/bin/bash
source ./env.sh

printmessage() {
  local now=`date +%Y-%m-%d\ %H:%M:%S.$(( $(date +%-N) / 1000000 ))`
  local message="$now $*"
  echo -e ${message} | tee -a ${repl_log_file}
  fi
}
