# Hive database replication

Script to replicate a single Hive database from an HDP cluster to another. 
Utilizes the HIVE REPL features used under the hood in DLM. Supports replication of managed, external tables and views.
Try running the manual steps in the ManualSteps.md document before running the script. For instructions, see the section below.
Tested on HDP 3.1.4. but it is recommended to use HDP 3.1.5 or CDP versions for mature Hive REPL implementation.

# Files :
* `env.sh` : Configure environment variables here before running the script.
* `hive3repl.sh` : Main script to invoke. Run as shown later in the doc.
* `beeline-functions.sh` : Functions that make beeline calls are defined here.
* `init.sh` : All global variables are initialized here. Do not change anything here.
* `repl-common.sh` : Some miscellaneous functions for logging etc are defined here.

# Configs
| Parameter | Description |
| ----------- | ----------- |
| target_jdbc_url      | JDBC URL for target cluster. Copy this value from the Ambari UI.       |
| source_jdbc_url   |  JDBC URL for source cluster. Copy this value from the Ambari UI.        |
| dblist      | # List of acceptable dbnames when passed via argument to script. This is for a sanity check to avoid accidental full dump generation in prod for mistyped target database names.       |
|include_external_tables|true/false|
|repl_root|location in source hdfs where dump data will be written. This is used only to verify REPL DUMP output starting suffix |
|source_hdfs_prefix|Prefix to access HDFS locations at source cluster as accessed from target. Can use the Namenode IP:port or cluster nameservice id. Eg. `hdfs://c2186-node2.coelab.cloudera.com:8020` or `hdfs://c2186`"|
|beeline_user|User running beeline. In kerberized environments this may be ignored.|
|TMP_DIR| Directory to store temporary files used for parsing beeline output. Default: ./tmp|
|LOG_DIR| Directory to write script logs.  Default: ./logs|

# Workflow
The workflow is as follows:

* Takes in a database name alongside other env variables. 
* Env variables are picked up from configuration file. 
* Looks up the DR Hive instance for the current replication status of that database.
* If it’s not present, it goes to the Production hive instance and generates a full dump of the database. 
    * The dump returns an HDFS path of the dump and the latest transaction id at Production.
    * Once the production dump is generated, it connects back to DR hive instance and passes the HDFS location to the DR instance to replicate the database.
    * The events are then replayed on the DR replica database instance.
    * Post replay, the replication status is looked up again in DR.
    * Based on a comparison of the Production transaction id and post replay transaction id at DR, displays success or failure message.
* If an existing version of database exists in DR, it connects to Production Hive instance and passes the last replicated id in DR to production 
    * The production Hive instance then generates an incremental dump of the database and returns an HDFS path of the dump and the latest transaction id at Production.
    * Once the production dump is generated, it connects back to the DR hive instance and the events are replayed on the DR replica database instance.
    * Post replay, the replication status is looked up again in DR, 
    * Based on a comparison of the Production transaction id and post replay transaction id at DR, displays success or failure message.
* All messages are printed both to console and a log file.

# Instructions to run 

1. Update configurations in env.sh
2. Ensure source database has the `repl.source.for` variable set. See ManualSteps.md for more details.
3. Ensure target cluster has the database created
4. If the cluster is kereberized, obtain the kerberos tickets.
5. Launch the script with the database name as argument. 
6. Track the script progress in the log filename which is printed in the console for each run.
7. Upon successfull completion, the updated transaction id at the target database will be displayed.

# Sample run 

First time - 
FULL DUMP 
```
[hive@c4186-node3 Hive_acid_table_replication]$ bash hive3repl.sh repltest 
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
INCREMENTAL DUMP 
```
[hive@c4186-node3 Hive_acid_table_replication]$ bash hive3repl.sh repltest 
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
