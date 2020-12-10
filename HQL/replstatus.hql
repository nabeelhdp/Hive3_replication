-- This will be run at the destination to retrieve current replication load_status
-- The transaction id obtained here will be used to generate the source database dumps
use ${hivevar:dbname};
repl status ${hivevar:dbname};
