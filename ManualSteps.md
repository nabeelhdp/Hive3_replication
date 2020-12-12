# Manual steps to verify end to end replication works on beeline shell.

In Ambari UI -> Hive -> CONFIGS
Advanced hive-site

```
hive.metastore.dml.events=true
hive.repl.cm.enabled=true 
hive.repl.rootdir=/apps/hive/repl 
hive.repl.cmrootdir=/apps/hive/cmroot
hive.metastore.transactional.event.listeners=org.apache.hive.hcatalog.listener.DbNotificationListener 
```
The communication paths involved for Replication to work would involve one beeline session that can talk to  zookeeper, HS2 and HDFS services in both clusters.

For a manual test, we need to do the following:
1) Create database in Production Hive using beeline
`create database repltest;`
 Create some tables in the database, and populate some dummy data.
2) Enable ChangeManager on Database. The '1' below can be any value. It's only to initiate the ChangeManager on the db.
`alter database repltest properties SET DBPROPERTIES ('repl.source.for'='1')`
3) Generate initial (bootstrap) replication dump for database
`repl dump repltest;`
4) Note the folder in the output from above, and on the DR cluster beeline session.
`repl load repltest from <folder-from-above-command>;`
5) Now check if the data inserted into tables in source have been replicated in destination.
6) Next insert some data into source tables.
7) Use command below in DR to get last_replication_id
`repl status repltest`
8) In prod beeline session generate dump from that point onwards
`repl dump repltest from <last_replication_id>`
9) Run Step 4 again.

This will help us verify that manual sync of ACID tables is working between the clusters. The script will use the above procedure to perform actions 3 -9.

There may be an error in step 3 in HDP versions upto 3.1.5.

Issue:
Step 3 fails with error: ReplicationChangeManager is not initialized

Workaround:
Create and drop a populated dummy table  in the database where we want to replicate the data.
This is a one time activity for the database and doesn't need to be repeated.  
