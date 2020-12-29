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
|beeline_user|User running beeline. In kerberized environments this may be ignored|
|TMP_DIR| Directory to store temporary files used for parsing beeline output|
|LOG_DIR| Directory to write script logs|
|HQL_DIR| Directory to hold all HiveQL script files.|

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
|beeline_opts| Options to pass to beeline when launching. To change, use DEBUG option when running the script. Do not change here, as it breaks output parsing. Defaults to `verbose=false --showHeader=false --silent=true`|

# Sample run 

First time - 
FULL DUMP  (interactive prompt added for safety. Full dumps can add significant file count and load at source)
```
[hive@c4186-node3 Hive_acid_table_replication]$ bash hive3-repl.sh repltest DEBUG
2020-12-28 08:28:00.672 Enabling DEBUG output
2020-12-28 08:28:00.678 Initiating run to replicate repltest to repltest
2020-12-28 08:28:05.852 No replication id detected at target. Full data dump dump needs to be initiated.
2020-12-28 08:28:05.857 Database repltest is being synced for the first time. Initiating full dump.
2020-12-28 08:28:05.861 Skipping external tables in full dump
2020-12-28 08:28:11.831 Source transaction id: |1029|
2020-12-28 08:28:11.837 Database repltest full dump has been generated at |hdfs://c2186-node2.coelab.cloudera.com:8020/apps/hive/repl/5d117227-38c4-4b1c-826b-e3222b9dfbc3|.
2020-12-28 08:28:11.843 The current transaction ID at source is |1029|
2020-12-28 08:28:11.847 There are 1029 transactions to be synced in this run.
2020-12-28 08:28:11.851 Initiating data load at target cluster on database repltest.
2020-12-28 08:28:11.856 External tables not included.
2020-12-28 08:28:31.926 Data load at target cluster completed. Verifying....
2020-12-28 08:28:37.368 Database replication completed SUCCESSFULLY. Last transaction id at target is |1029|
```
INCREMENTAL DUMP (interactive prompt added for safety. Full dumps can add significant file count and load at source)
```

[hive@c4186-node3 Hive_acid_table_replication]$ bash hive3-repl.sh repltest DEBUG
2020-12-28 08:29:01.290 Enabling DEBUG output
2020-12-28 08:29:01.297 Initiating run to replicate repltest to repltest
2020-12-28 08:29:06.402 Database repltest transaction ID at target is currently |1029|
2020-12-28 08:29:06.407 Skipping external tables in incremental dump
2020-12-28 08:29:22.185 The current transaction ID at source is |1034|
2020-12-28 08:29:22.190 Database repltest incremental dump has been generated at |hdfs://c2186-node2.coelab.cloudera.com:8020/apps/hive/repl/276b8090-7ddf-43ed-929b-6adc40f42249|.
2020-12-28 08:29:22.196 There are 5 transactions to be synced in this run.
2020-12-28 08:29:22.201 External tables not included.
2020-12-28 08:29:41.424 Data load at target cluster completed. Verifying....
2020-12-28 08:29:46.790 Database replication completed SUCCESSFULLY. Last transaction id at target is |1034|
```
Console output won't differ much in non-DEBUG mode. The extra logging will be in the log file at the backend.
```
[hive@c4186-node3 Hive_acid_table_replication]$ bash hive3-repl.sh repltest 
2020-12-28 08:29:52.253 Initiating run to replicate repltest to repltest
2020-12-28 08:29:57.346 Database repltest transaction ID at target is currently |1034|
2020-12-28 08:29:57.352 Skipping external tables in incremental dump
2020-12-28 08:30:02.849 The current transaction ID at source is |1036|
2020-12-28 08:30:02.856 Database repltest incremental dump has been generated at |hdfs://c2186-node2.coelab.cloudera.com:8020/apps/hive/repl/064b89f9-1665-49c0-b945-3da3990c00fe|.
2020-12-28 08:30:02.862 There are 2 transactions to be synced in this run.
2020-12-28 08:30:02.867 External tables not included.
2020-12-28 08:30:21.742 Data load at target cluster completed. Verifying....
2020-12-28 08:30:26.677 Database replication completed SUCCESSFULLY. Last transaction id at target is |1036|
[hive@c4186-node3 Hive_acid_table_replication]$ 
```
