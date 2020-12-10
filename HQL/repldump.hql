-- This will be run at the source to generate incremental database dumps
use ${hivevar:dbname};
repl dump ${hivevar:dbname} from ${hivevar:last_repl_id};
