-- This will be run at the source to generate full database dumps 
use ${hivevar:dbname};
repl dump ${hivevar:dbname};
