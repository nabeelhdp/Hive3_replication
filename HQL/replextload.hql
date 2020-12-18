-- This will be run at the target to replay all events in the dump
-- folder that was generated at the source. The path for the source
-- will point to the source cluster hdfs location.
use ${hivevar:dbname};
repl load ${hivevar:dbname} from '${hivevar:src_dump_path}' with (
'hive.repl.replica.external.table.base.dir'='/prod_hive' );
