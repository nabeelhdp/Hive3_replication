# Hive acid table replication

Script to replicate a single database from an HDP cluster to another.
Generally used for Prod to DR sync.
Utilizes the HIVE REPL features used under the hood in DLM.
Try running the manual steps in the ManualSteps.md document before running the script.

# Recommended : 
Use HDP 3.1.5 or CDP versions to use this script.

# Configs
| Parameter      | Description |
| ----------- | ----------- |
| target_jdbc_url      | JDBC URL for target cluster. Copy this value from the Ambari UI.       |
| source_jdbc_url   |  JDBC URL for source cluster. Copy this value from the Ambari UI.        |
| dblist      | # List of acceptable dbnames when passed via argument to script. This is for a sanity check to avoid accidental full dump generation in prod for mistyped target database names.       |
|include_external_tables|true/false|
|repl_root|location in source hdfs where dump data will be written. This is used only to verify REPL DUMP output starting suffix |
|source_hdfs_prefix|Prefix to access HDFS locations at source cluster as accessed from target. Eg. `hdfs://c2186-node2.coelab.cloudera.com:8020`"|
|beeline_opts|Options to pass to beeline when launching . Defaults to `verbose=false --showHeader=false --silent=true`|
|beeline_user|User running beeline. In kerberized environments this may be ignored|
|TMP_DIR| Location to store temporary files used for parsing beeline output|
|LOG_DIR| Location to write script logs|
|HQL_DIR|./HQL|

| Locations for the various Hive QL scripts for each action. DO NOT CHANGE| |
| ----------- | ----------- |
|INC_DUMP_HQL|${HQL_DIR}/repldump.hql|
|BOOTSTRAP_HQL|${HQL_DIR}/replbootstrap.hql|
|EXT_INC_DUMP_HQL|${HQL_DIR}/replextdump.hql|
|EXT_BOOTSTRAP_HQL|${HQL_DIR}/replextbootstrap.hql|
|LOAD_HQL|${HQL_DIR}/replload.hql|
|EXT_LOAD_HQL|${HQL_DIR}/replextload.hql|
|STATUS_HQL|${HQL_DIR}/replstatus.hql|
|repl_log_file|${LOG_DIR}/replication_$(date +"%Y_%m_%d_%I_%M_%p").log"|
# Sample run 

First time - 
FULL DUMP  (interactive prompt added for safety. Full dumps can add significant file count and load at source)
```
[hive@c4186-node3 ACID COPY]$ bash acidrepl.sh repltest_replica
2020-12-11 06:00:02.471 ===================================================================
2020-12-11 06:00:02.476 Initiating run to replicate repltest to repltest_replica
2020-12-11 06:00:02.480 ===================================================================
2020-12-11 06:00:07.393 No replication id detected at target. Full data dump dump needs to be initiated.
2020-12-11 06:00:12.781 Database repltest is being synced for the first time. Initiating full dump.
2020-12-11 06:00:28.454 Source transaction id: |517|
2020-12-11 06:00:28.461 Database repltest full dump has been generated at hdfs://c2186-node2.coelab.cloudera.com:8020/apps/hive/repl/62f21edc-6fee-4eb1-a7ce-26fee07f3516.
2020-12-11 06:00:28.468 The current transaction ID at source is 517
2020-12-11 06:00:28.475 There are 517 transactions to be synced in this run.
2020-12-11 06:00:28.481 Initiating data load at target cluster on database repltest_replica.
2020-12-11 06:00:39.150 Database synchronized successfully. Last transaction id at target is 517
```
INCREMENTAL DUMP (interactive prompt added for safety. Full dumps can add significant file count and load at source)
```
[hive@c4186-node3 ACID COPY]$ bash acidrepl.sh repltest_replica
2020-12-11 06:00:44.944 ===================================================================
2020-12-11 06:00:44.949 Initiating run to replicate repltest to repltest_replica
2020-12-11 06:00:44.955 ===================================================================
2020-12-11 06:00:50.80 Database repltest transaction ID at target is currently 517
2020-12-11 06:01:13.315 Source transaction id: |522|
2020-12-11 06:01:13.319 Database repltest incremental dump has been generated at hdfs://c2186-node2.coelab.cloudera.com:8020/apps/hive/repl/08406a86-013c-4a77-97dc-06ffd1b79fca.
2020-12-11 06:01:13.324 The current transaction ID at source is 522
2020-12-11 06:01:13.327 There are 5 transactions to be synced in this run.
2020-12-11 06:01:23.106 Database synchronized successfully. Last transaction id at target is 522
[hive@c4186-node3 ACID COPY]$ ll
```
