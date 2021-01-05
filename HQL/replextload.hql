-- This will be run at the target to replay all events in the dump
-- folder that was generated at the source. The path for the source
-- will point to the source cluster hdfs location.
use ${hivevar:dbname};
repl load ${hivevar:dbname} from '${hivevar:src_dump_path}' with (
'hive.exec.parallel'='true',
'hive.exec.parallel.thread.number'='128',
'hive.repl.parallel.copy.tasks'='500',
-- 'hive.repl.replica.external.table.base.dir'='',
-- 'hive.distcp.privileged.doAs'='beacon',
'distcp.options.pugprb'='',
'distcp.options.skipcrccheck'='',
'distcp.options.update'='');
