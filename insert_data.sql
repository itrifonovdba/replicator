--run on source database to insert into  rpadmin.replicator_tables on destination for replication

select 'insert into rpadmin.replicator_tables values ( '''||table_owner ||''','''||table_name||''','''|| partition_name||''',1,'''','''',99,'''','''','''',''RPRIME'')'
 from dba_tab_partitions where table_owner=''  and partition_name like '%2018%'  order by  table_name,partition_name
