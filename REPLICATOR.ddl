
  CREATE OR REPLACE  PACKAGE "RPADMIN"."REPLICATOR" 
as 
procedure REPLICATOR_RUN(in_thread varchar2) ;


PROCEDURE SendMail (p_to          IN VARCHAR2,
                    p_from        IN VARCHAR2,
                    p_subject     IN VARCHAR2,
                    p_text_msg    IN VARCHAR2 DEFAULT NULL,
                    p_attach_name IN VARCHAR2 DEFAULT NULL,
                    p_attach_mime IN VARCHAR2 DEFAULT NULL,
                    p_attach_clob IN CLOB DEFAULT NULL,
                    p_smtp_host   IN VARCHAR2,
                    p_smtp_port   IN NUMBER DEFAULT 25);


end ;





CREATE OR REPLACE  PACKAGE BODY "RPADMIN"."REPLICATOR" 
as 


PROCEDURE SendMail (p_to          IN VARCHAR2,
                                       p_from        IN VARCHAR2,
                                       p_subject     IN VARCHAR2,
                                       p_text_msg    IN VARCHAR2 DEFAULT NULL,
                                       p_attach_name IN VARCHAR2 DEFAULT NULL,
                                       p_attach_mime IN VARCHAR2 DEFAULT NULL,
                                       p_attach_clob IN CLOB DEFAULT NULL,
                                       p_smtp_host   IN VARCHAR2,
                                       p_smtp_port   IN NUMBER DEFAULT 25)
IS
  l_mail_conn   UTL_SMTP.connection;
  l_boundary    VARCHAR2(50) := '----=*#abc1234321cba#*=';
  l_step        PLS_INTEGER  := 12000; -- make sure you set a multiple of 3 not higher than 24573
BEGIN
  l_mail_conn := UTL_SMTP.open_connection(p_smtp_host, p_smtp_port);
  UTL_SMTP.helo(l_mail_conn, p_smtp_host);
  UTL_SMTP.mail(l_mail_conn, p_from);
  UTL_SMTP.rcpt(l_mail_conn, p_to);

  UTL_SMTP.open_data(l_mail_conn);
  
  UTL_SMTP.write_data(l_mail_conn, 'Date: ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS') || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'To: ' || p_to || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'From: ' || p_from || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'Subject: ' || p_subject || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'Reply-To: ' || p_from || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'MIME-Version: 1.0' || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'Content-Type: multipart/mixed; boundary="' || l_boundary || '"' || UTL_TCP.crlf || UTL_TCP.crlf);
  
  IF p_text_msg IS NOT NULL THEN
    UTL_SMTP.write_data(l_mail_conn, '--' || l_boundary || UTL_TCP.crlf);
    UTL_SMTP.write_data(l_mail_conn, 'Content-Type: text/plain; charset="iso-8859-1"' || UTL_TCP.crlf || UTL_TCP.crlf);

    UTL_SMTP.write_data(l_mail_conn, p_text_msg);
    UTL_SMTP.write_data(l_mail_conn, UTL_TCP.crlf || UTL_TCP.crlf);
  END IF;

  IF p_attach_name IS NOT NULL THEN
    UTL_SMTP.write_data(l_mail_conn, '--' || l_boundary || UTL_TCP.crlf);
    UTL_SMTP.write_data(l_mail_conn, 'Content-Type: ' || p_attach_mime || '; name="' || p_attach_name || '"' || UTL_TCP.crlf);
    UTL_SMTP.write_data(l_mail_conn, 'Content-Disposition: attachment; filename="' || p_attach_name || '"' || UTL_TCP.crlf || UTL_TCP.crlf);
 
    FOR i IN 0 .. TRUNC((DBMS_LOB.getlength(p_attach_clob) - 1 )/l_step) LOOP
      UTL_SMTP.write_data(l_mail_conn, DBMS_LOB.substr(p_attach_clob, l_step, i * l_step + 1));
    END LOOP;

    UTL_SMTP.write_data(l_mail_conn, UTL_TCP.crlf || UTL_TCP.crlf);
  END IF;
  
  UTL_SMTP.write_data(l_mail_conn, '--' || l_boundary || '--' || UTL_TCP.crlf);
  UTL_SMTP.close_data(l_mail_conn);

  UTL_SMTP.quit(l_mail_conn);
END SendMail;





procedure REPLICATOR_RUN (in_thread varchar2)
is
/*
v.04-05-2021  need grant SELECT ANY SEQUENCE  for tables with identity columns +grant create any sequence to rpadmin
*/

    ind NUMBER;                  -- Loop index number
    h1 number;                   -- Data Pump job handle
    percent_done number := 0;    -- Percentage of job complete
    job_state  varchar2(100) := 'UNDEFINED';    --    Keeps track of job state
    le ku$_LogEntry;             -- work-in-progress and error messages
    jd ku$_JobDesc;              -- Job description from get_status
    sts ku$_Status;              -- Status object returned by get_status
    v_curr_scn number:=0;
    jb number(4);
    v_sts ku$_Status;            -- The status object returned by get_status
    js ku$_JobStatus;            -- The job status from get_status
    v_ex clob;
    v_tex varchar2 (120);
    v_mx number(16);
    v_readBuf VARCHAR2(512);
    h2 UTL_FILE.FILE_TYPE;
    l_clob  clob;
    v_db varchar2 (120);
    v_fnd number (1):=0;
    v_owner sys.dba_indexes.owner%TYPE;
    v_index_name  sys.dba_indexes.index_name%TYPE;
    sysrefcursor1  SYS_REFCURSOR;
    v_link_name varchar2(120);
    already_indexed EXCEPTION;
    PRAGMA EXCEPTION_INIT(already_indexed , -1408);
BEGIN

    for z in (
      select table_owner,table_name,partition_name,pk from  RPADMIN.REPLICATOR_TABLES 
      where enabled=1 and thread=in_thread and pk is not null order by table_name,partition_name
    ) 
    loop
    v_ex:='select max('||z.pk||') from '||z.table_owner||'.'||z.table_name ;
     execute immediate v_ex into v_mx;
      update RPADMIN.REPLICATOR_TABLES set filter=' '||z.pk||'>'||v_mx  
      where table_owner=z.table_owner and table_name=z.table_name and partition_name=z.partition_name;  
    end loop; commit;


  SELECT current_scn INTO v_curr_scn FROM sys.v_$database;                                                                                                                                                            

    for x in (
      select /*table_owner, table_name, partition_name,filter,remap_table*/ * from  RPADMIN.REPLICATOR_TABLES where enabled=1 and thread=in_thread order by table_name,partition_name
    ) loop
  
SELECT trunc(DBMS_RANDOM.value(1,1000 ))  into jb FROM dual;

select upper(trim(x.source_database_link_name)) into v_link_name from dual;
 

  begin
   select table_name  into v_tex from sys.dba_tables where table_name=x.table_name and owner=x.table_owner; 
    exception when no_data_found then  
     select  rpadmin.read_remote_ddl('TABLE',x.table_name,x.table_owner,v_link_name) into v_ex from  dual;
     dbms_output.put_line(v_ex);
      execute immediate v_ex;
     

        open  sysrefcursor1  for ('select owner,index_name from sys.dba_indexes@'||v_link_name||' where table_name='''||x.table_name||''' and owner='''||x.table_owner||''' '); 
           loop 
            fetch sysrefcursor1 into v_owner,v_index_name ;
              EXIT WHEN  sysrefcursor1%NOTFOUND;
               select  read_remote_ddl('INDEX',v_index_name,v_owner,v_link_name) into v_ex from  dual;
                   begin
                    execute immediate v_ex;
                     exception
                      WHEN already_indexed  then 
                       dbms_output.put_line('#########################  ####################');
                   end;  
              end loop;
             close sysrefcursor1;
           
              
  end; 



                                
  h1 := dbms_datapump.open ( operation=>'IMPORT',
                                job_mode=>'TABLE',
                                remote_link=>v_link_name,
                                job_name=> 'RP2A_'||x.table_name||'_'||jb
                                );
                                
                                        

 dbms_datapump.add_file(
HANDLE=>h1, 
FILENAME=>'imp_'||x.table_name||'_'||x.partition_name||'.log', 
DIRECTORY=>'DBMS_OPTIM_LOGDIR',
filetype=>dbms_datapump.KU$_FILE_TYPE_LOG_FILE,
REUSEFILE=>0);


for y in ( select src_tbs,dst_tbs from RPADMIN.REPLICATOR_REMAP_TABLESPACE ) loop
 DBMS_DATAPUMP.METADATA_REMAP(h1,'REMAP_TABLESPACE',y.src_tbs,y.dst_tbs);
 end loop;
 
         dbms_datapump.metadata_filter (
            handle => h1,
            name   => 'SCHEMA_EXPR',
            value  => 'IN ('''||x.table_owner||''')');
            
        dbms_datapump.metadata_filter (
            handle => h1,
            name   => 'NAME_EXPR',
            value  => 'IN ('''||x.table_name||''')');
            

DBMS_DATAPUMP.SET_PARAMETER (h1,'DATA_OPTIONS',DBMS_DATAPUMP.KU$_DATAOPT_ENABLE_NET_COMP);    
   

if (x.partition_name is not null and x.filter is null ) then  --1 

begin  
sys.trnc('TRUNCATE',x.table_owner,x.table_name,x.partition_name,'');
 end;

        dbms_datapump.data_filter (
            handle      => h1,
            name        => 'PARTITION_LIST',
            value       => x.partition_name,
            table_name  => x.table_name,
            schema_name => x.table_owner);
            
  DBMS_DATAPUMP.SET_PARAMETER(h1,'TABLE_EXISTS_ACTION','APPEND');

elsif (x.partition_name is not null and x.filter is not null) then --2

   sys.trnc('DELETE',x.table_owner,x.table_name,x.partition_name,x.filter);
    commit;
        dbms_datapump.data_filter (
            handle      => h1,
            name        => 'PARTITION_LIST',
            value       => x.partition_name,
            table_name  => x.table_name,
            schema_name => x.table_owner);
            
  DBMS_DATAPUMP.SET_PARAMETER(h1,'TABLE_EXISTS_ACTION','APPEND');


     DBMS_DATAPUMP.DATA_FILTER (
        handle => h1,
        name  => 'SUBQUERY',
        value => 'WHERE '||x.filter,
        table_name => x.table_name,
        schema_name => x.table_owner);

elsif (x.partition_name is null and x.filter is null) then --3
   DBMS_DATAPUMP.SET_PARAMETER(h1,'TABLE_EXISTS_ACTION','TRUNCATE');
elsif (x.partition_name is null and x.filter is not null) then --4
   DBMS_DATAPUMP.SET_PARAMETER(h1,'TABLE_EXISTS_ACTION','APPEND');


   sys.trnc('DELETE',x.table_owner,x.table_name,'',x.filter);
    commit;
     DBMS_DATAPUMP.DATA_FILTER (
        handle => h1,
        name  => 'SUBQUERY',
        value => 'WHERE '||x.filter,
        table_name => x.table_name,
        schema_name => x.table_owner);

    end if;
           
dbms_datapump.set_parameter(handle => h1, name => 'INCLUDE_METADATA', value => 1); 

            
DBMS_DATAPUMP.METADATA_FILTER(h1,
                              'EXCLUDE_PATH_EXPR',
                              'IN (''AUDIT'',''PASSWORD_HISTORY'',''STATISTICS'',''TABLE_EXPORT/TABLE/COMMENT'',''TABLE_EXPORT/TABLE/RLS_POLICY/RLS_POLICY'',''TRIGGER'',''OBJECT_GRANT'',''PROCDEPOBJ'',''PROCACT_INSTANCE'',''PROCDEPOBJ_GRANT'',''INDEX_STATISTICS'',''TABLE_STATISTICS'',''OBJECT_GRANT'',''CONSTRAINT'',''REF_CONSTRAINT'')');


if  x.remap_table is not null then
DBMS_DATAPUMP.METADATA_REMAP (
handle => h1,
name => 'REMAP_TABLE',
old_value => x.table_name,
value =>x.remap_table);
end if;

   
    dbms_datapump.start_job (handle =>h1);
    

    
     if x.partition_name is not null then
       update RPADMIN.REPLICATOR_TABLES  set start_refresh_time=sysdate 
        where table_owner=x.table_owner and  table_name=x.table_name and partition_name=x.partition_name;
     else  
       update RPADMIN.REPLICATOR_TABLES  set start_refresh_time=sysdate 
        where table_owner=x.table_owner and  table_name=x.table_name ;
     end if;
     commit;
 
    
   
begin

   percent_done := 0;
    job_state := 'undefined';
    while (job_state != 'completed') and (job_state != 'stopped') and   (job_state != 'not running') loop
       dbms_datapump.get_status(h1,
          dbms_datapump.ku$_status_job_error +
          dbms_datapump.ku$_status_job_status +
          dbms_datapump.ku$_status_wip,-1,job_state,v_sts);
          
          DBMS_OUTPUT.PUT_LINE('job_state='||job_state);


         DBMS_LOCK.SLEEP(5);
        js := v_sts.job_status;
   
        if js.percent_done != percent_done
        then
          dbms_output.put_line('*** Job percent done = ' ||
                               to_char(js.percent_done));
          percent_done := js.percent_done;
        end if;
  
   
       if (bitand(sts.mask,dbms_datapump.ku$_status_wip) != 0)
    then
      le := sts.wip;
                    dbms_output.put_line('error1');

    else
      if (bitand(sts.mask,dbms_datapump.ku$_status_job_error) != 0)
      then
              dbms_output.put_line('error2');

        le := sts.error;
      else
        le := null;
      end if;
    end if;
    if le is not null
    then
                  dbms_output.put_line('error3');
      ind := le.FIRST;
      while ind is not null loop
        dbms_output.put_line(le(ind).LogText);
        ind := le.NEXT(ind);
      end loop;
    end if;
   
    end loop;
   
        dbms_datapump.detach (handle => h1);
        

exception  when others then null;
  v_ex:=SQLERRM;

dbms_output.put_line(v_ex);

end;


begin

SELECT SYS_CONTEXT ('USERENV','DB_UNIQUE_NAME') into  v_db from dual;
begin
v_fnd:=0;
l_clob:='';
h2 := UTL_FILE.FOPEN('DBMS_OPTIM_LOGDIR','imp_'||x.table_name||'_'||x.partition_name||'.log','R');
  if UTL_FILE.IS_OPEN(h2) THEN
      Loop
              UTL_FILE.GET_LINE(h2,v_readBuf);
              l_clob:=l_clob||v_readBuf||chr(13);
              if regexp_like(v_readBuf,'ORA-','i') then
                
                if not regexp_like(v_readBuf,'ORA-39181','i')  then
                        if not  regexp_like(v_readBuf,'ORA-39185' ,'i')  then
                         v_fnd:=1; 
                        end if;        
                 end if;  
                              
               end if;
   
              dbms_output.put_line(v_readBuf);
      end loop;
   end if;   
exception when others then null;
end;  
      
      if v_fnd>0 then
                                      import_from_prime.SendMail(  
                                p_to          => 'dba@gmail.com',
                                p_from        => 'replicator@gmail.com',
                                p_subject     => 'Replication alert from'||v_db,
                                p_text_msg    => 'This is a automatic message.Do not reply! '||chr(10)||
                                'Please find attached file for details' ,
                                p_attach_name => 'failed_replication_job_details.txt',
                                p_attach_mime => 'text/plain',
                                p_attach_clob => l_clob,
                                p_smtp_host   => 'smtp.organization.org');

      
        end if;
        EXCEPTION WHEN others THEN null; 
END;



if x.partition_name is not null then
 update RPADMIN.REPLICATOR_TABLES  set refresh_time=sysdate 
 where table_owner=x.table_owner and  table_name=x.table_name and partition_name=x.partition_name;
else  
 update RPADMIN.REPLICATOR_TABLES  set refresh_time=sysdate 
 where table_owner=x.table_owner and  table_name=x.table_name ;
end if;
commit;
end loop;
end REPLICATOR_RUN;
end ;


