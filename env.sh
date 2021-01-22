#!/bin/bash

# JDBC URLs from both clusters. Copy this value from the Ambari UI.
target_jdbc_url="jdbc:hive2://c4186-node2.coelab.cloudera.com:2181,c4186-node3.coelab.cloudera.com:2181,c4186-node4.coelab.cloudera.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
source_jdbc_url="jdbc:hive2://c2186-node2.coelab.cloudera.com:2181,c2186-node3.coelab.cloudera.com:2181,c2186-node4.coelab.cloudera.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

# List of acceptable dbnames when passed via argument to script. This is for a sanity check to avoid accidental full dump generation in prod for mistyped target database names.
dblist="repltest"

# The default behaviour of the REPL command excludes external tables. Set this to true to include external tables in the replication process.
include_external_tables=false   # [true|false]

# location in source hdfs where dump data will be written. This is used only to verify REPL DUMP output starting suffix
repl_root="/apps/hive/repl"

# Prefix to access HDFS locations at source cluster as accessed from target
#source_hdfs_prefix="hdfs://c2186"
source_hdfs_prefix="hdfs://c2186-node2.coelab.cloudera.com:8020"

# User running beeline. In kerberized environments this may be ignored.
beeline_user="hive"

# Number of attempts to retry a failed incremental repl load
INCR_RERUN=3

# Flag to apply workaround for HDP 3.1.4 for this error
# https://docs.cloudera.com/HDPDocuments/DLM1/DLM-1.5.1/administration/content/dlm_replchangemanager_error.html
initReplChangeManager=true  # [true|false]

# The features below are not tested thoroughly, so enable only if you can fix subsequent errors
# Default for all these  is set to false accordingly.
APPLY_DB_LOCK=false # [true|false]
# HDFS Location for log file  upload at end of replication run.
# Enable this flag to upload
HDFS_UPLOAD=false  # [true|false]
# Create this folder before configuring it here.
hdfs_upload_dir=

