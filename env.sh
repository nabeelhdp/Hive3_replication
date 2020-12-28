#!/bin/bash

# JDBC URLs from both clusters. Copy this value from the Ambari UI.
target_jdbc_url="jdbc:hive2://c4186-node2.coelab.cloudera.com:2181,c4186-node3.coelab.cloudera.com:2181,c4186-node4.coelab.cloudera.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
source_jdbc_url="jdbc:hive2://c2186-node2.coelab.cloudera.com:2181,c2186-node3.coelab.cloudera.com:2181,c2186-node4.coelab.cloudera.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

# List of acceptable dbnames when passed via argument to script. This is for a sanity check to avoid accidental full dump generation in prod for mistyped target database names.
dblist="repltest repltest_replica"

include_external_tables=false
repl_root="/apps/hive/repl"
#source_hdfs_prefix="hdfs://c2186"
source_hdfs_prefix="hdfs://c2186-node2.coelab.cloudera.com:8020"
beeline_opts="--verbose=false --showHeader=false --silent=true"
beeline_user="hive"

# Locations for the various Hive QL scripts for each action.
HQL_DIR="./HQL"
INC_DUMP_HQL="${HQL_DIR}/repldump.hql"
BOOTSTRAP_HQL="${HQL_DIR}/replbootstrap.hql"
EXT_INC_DUMP_HQL="${HQL_DIR}/replextdump.hql"
EXT_BOOTSTRAP_HQL="${HQL_DIR}/replextbootstrap.hql"
LOAD_HQL="${HQL_DIR}/replload.hql"
EXT_LOAD_HQL="${HQL_DIR}/replextload.hql"
STATUS_HQL="${HQL_DIR}/replstatus.hql"

TMP_DIR="./tmp/run_$(date +"%Y_%m_%d_%I_%M_%p")"
LOG_DIR="./logs"
repl_log_file="${LOG_DIR}/replication_$(date +"%Y_%m_%d_%I_%M_%p").log"
