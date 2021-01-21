-- This is a workaround for a bug in HDP 3.1.4 
-- See https://docs.cloudera.com/HDPDocuments/DLM1/DLM-1.5.1/administration/content/dlm_replchangemanager_error.html
use ${hivevar:dbname};
create table if not exists replChange_dummy(id int, name string);
insert into replChange_dummy values(1,"dummy value");
drop table if exists replChange_dummy;
