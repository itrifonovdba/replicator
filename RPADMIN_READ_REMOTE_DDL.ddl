
  CREATE OR REPLACE  FUNCTION "RPADMIN"."READ_REMOTE_DDL" ( p_type varchar2,p_name varchar2,p_schema varchar2,p_link varchar2 default null)
RETURN CLOB IS
   c_ddl CLOB;
   v_ddl varchar(4000);
   v_link varchar2(30);
   v_sql varchar(400);
   v_len number;
   h1 number;
   th1 number;
   ddltext clob;
   PkgTable SYS.KU$_DDLS;
BEGIN

IF p_link is null
THEN
   h1 := dbms_metadata.open(OBJECT_TYPE=>p_type);
else 
   h1 := dbms_metadata.open(OBJECT_TYPE=>p_type,NETWORK_LINK=>p_link);
end if;   
          DBMS_METADATA.SET_FILTER(   h1,'SCHEMA',p_schema);
          dbms_metadata.set_filter(h1,'NAME',p_name);
   th1 := dbms_metadata.add_transform(h1, 'DDL');
          dbms_metadata.set_transform_param(th1,'PRETTY', TRUE);
          dbms_metadata.set_transform_param(th1,'SQLTERMINATOR', false);
          dbms_metadata.set_transform_param(th1,'SEGMENT_ATTRIBUTES',false);
          dbms_metadata.set_transform_param(th1,'STORAGE',false); 
          dbms_metadata.set_transform_param(th1,'TABLESPACE',false);

if  p_type='TABLE' then
   dbms_metadata.set_transform_param(th1,'CONSTRAINTS',true);
   dbms_metadata.set_transform_param(th1,'REF_CONSTRAINTS',false);
   dbms_metadata.set_transform_param(th1,'CONSTRAINTS_AS_ALTER',false);
end if;

PkgTable:=DBMS_METADATA.FETCH_DDL(h1);
if (PkgTable is NOT NULL) then
ddltext := PkgTable(1).ddlText;
end if;

RETURN ddltext;
END;