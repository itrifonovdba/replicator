--create job for every replication thread 



BEGIN
  DBMS_SCHEDULER.create_job (
    job_name        => 'RPADMIN.REPLICATION_JOB_1',
    job_type        => 'PLSQL_BLOCK',
    job_action      => 'begin RPADMIN.REPLICATOR.REPLICATOR_RUN(1);end;',  -- here we set job/thread number
    start_date      =>  to_date('12-09-2020 00:01:00','DD-MM-YYYY HH24:MI:SS'),
    repeat_interval => 'FREQ=WEEKLY;BYDAY=TUE,WED,THU,FRI,SAT,SUN; BYHOUR=0; BYMINUTE=1; BYSECOND=0;',
    enabled         => TRUE);
END;

...

BEGIN
  DBMS_SCHEDULER.create_job (
    job_name        => 'RPADMIN.REPLICATION_JOB_99',
    job_type        => 'PLSQL_BLOCK',
    job_action      => 'begin RPADMIN.REPLICATOR.REPLICATOR_RUN(99);end;',
    start_date      =>  to_date('12-09-2020 00:01:00','DD-MM-YYYY HH24:MI:SS'),
    repeat_interval => 'FREQ=WEEKLY;BYDAY=TUE,WED,THU,FRI,SAT,SUN; BYHOUR=0; BYMINUTE=1; BYSECOND=0;',
    enabled         => TRUE);
END;



