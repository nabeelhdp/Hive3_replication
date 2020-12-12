# Hive acid table replication

Script to replicate a single database from an HDP cluster to another.
Generally used for Prod to DR sync.
Utilizes the HIVE REPL features used under the hood in DLM.
Try running the manual steps in the ManualSteps.md document before running the script.

Sample run 

First time - 
FULL DUMP  (interactive prompt added for safety. Full dumps can add significant file count and load at source)
```
[hive@c4186-node3 ACID COPY]$ bash acidrepl.sh repltest_replica
2020-12-11 06:00:02.471 ===================================================================
2020-12-11 06:00:02.476 Initiating run to replicate repltest to repltest_replica
2020-12-11 06:00:02.480 ===================================================================
2020-12-11 06:00:07.393 No replication id detected at target. Full data dump dump needs to be initiated.
Continue with full dump ? Y:N 
Y

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
