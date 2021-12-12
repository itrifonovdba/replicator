  CREATE OR REPLACE PROCEDURE "SYS"."TRNC" (in_oper in varchar2, in_owner in varchar2 , in_table  in varchar2 , in_part varchar2,in_filter in varchar2)
as 
begin
if in_oper='TRUNCATE' then
execute immediate 'alter table '||in_owner||'.'||in_table||' truncate partition "'||in_part||'"';
--DBMS_OUTPUT.PUT_LINE('truncate...............');
elsif 
in_oper='DELETE' then
   if  in_part is not null then 
   execute immediate 'delete '||in_owner||'.'||in_table||' partition ("'|| in_part ||'")'|| ' where '||in_filter;
   else
   execute immediate 'delete '||in_owner||'.'||in_table||' where '||in_filter;
   end if;
--DBMS_OUTPUT.PUT_LINE('delete...............');
end if; 
end;


