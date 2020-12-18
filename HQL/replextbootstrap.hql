-- This will be run at the source to generate incremental database dumps
use ${hivevar:dbname};
repl dump ${hivevar:dbname} with (
'hive.repl.include.external.tables'='true'
);
