application:start(erloci).
{ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param).

f(OciPort).
OciPort = erloci:new([{logging, true}, {env, []}]).
f(OciSession).
OciSession = OciPort:get_session(Tns, User, Pswd).
f(SelStmt).
SelStmt = OciSession:prep_sql("select * from bin_fraction").
SelStmt:exec_stmt().
SelStmt:fetch_rows(100).
SelStmt:close().
OciPort:close().

f(SelStmt).
SelStmt = OciSession:prep_sql("select to_char(sysdate, 'DD-MM-YYYY HH24:MI:SS'), sysdate from dual").
SelStmt:exec_stmt().
SelStmt:fetch_rows(100).
SelStmt:close().

CreateProcedure = OciSession:prep_sql(<<"create or replace procedure myproc
    (p_cur out sys_refcursor)
    is
    begin
       open p_cur for select * from test1;
    end;
    ">>).
CreateProcedure:exec_stmt().
CreateProcedure:close().
f(CreateProcedure).

f(ExecStmt).
ExecStmt = OciSession:prep_sql(<<"begin myproc(:cursor); end;">>).
ExecStmt = OciSession:prep_sql(<<"call myproc(:p_first,:p_second,:p_result)">>).
ExecStmt = OciSession:prep_sql(<<"declare begin myproc(:p_first,:p_second,:p_result); end;">>).

ExecStmt:bind_vars([ {<<":cursor">>, out, 'SQLT_RSET'}]).
{executed, 1, [{<<":cursor">>, CurStmt}]} = ExecStmt:exec_stmt().
CurStmt:exec_stmt().
CurStmt:fetch_rows(100).
CurStmt:close().
ExecStmt:close().



OciSession:describe(<<"test1">>, 'OCI_PTYPE_TABLE').
OciSession:describe(<<"person_typ">>, 'OCI_PTYPE_TYPE').

application:start(erloci).

f().
OciPort = erloci:new([{logging, true}, {env, []}]).
Tns = <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>.
Pswd= <<"regit">>.
User= <<"scott">>.
OciSession = OciPort:get_session(Tns, User, Pswd).
SelStmt = OciSession:prep_sql("select :a from dual").
SelStmt:bind_vars([{<<":a">>,'SQLT_INT'}]).
SelStmt:exec_stmt([{10}]).
SelStmt:fetch_rows(100).

OciSession:ping().
SelStmt = OciSession:prep_sql("select :a from dual").
OciSession:ping().
SelStmt:exec_stmt().
OciSession:ping().
SelStmt:fetch_rows(100).
OciSession:ping().

f(SelStmt).
SelStmt = OciSession:prep_sql("select * from datetime").
SelStmt = OciSession:prep_sql("select * from bin_fraction").
SelStmt:exec_stmt().
SelStmt:fetch_rows(100).
SelStmt:close().
OciPort:close().

f(InsertStmt).
InsertStmt = OciSession:prep_sql(<<"insert into nums (bf, bd) values (:bf, :bd)">>).
InsertStmt:bind_vars([{<<":bf">>,'SQLT_BFLOAT'}, {<<":bd">>,'SQLT_BDOUBLE'}]).
InsertStmt:exec_stmt([{1.0e38,1.0e308},{1.0e-38,1.0e-308},{-1.0e-38,-1.0e-308},{-1.0e38,-1.0e308}]).

InsertStmt = OciSession:prep_sql(<<"insert into nums (bf) values (:bf)">>).
InsertStmt:bind_vars([{<<":bf">>,'SQLT_BFLOAT'}]).
InsertStmt:exec_stmt([{1.0e38},{1.0e-38},{-1.0e-38},{-1.0e38}]).

InsertStmt = OciSession:prep_sql(<<"insert into nums (bd) values (:bd)">>).
InsertStmt:bind_vars([{<<":bd">>,'SQLT_BDOUBLE'}]).
InsertStmt:exec_stmt([{1.0e308},{1.0e-308},{-1.0e-308},{-1.0e308}]).

InsertStmt:close().

f().
OciPort = oci_port:start_link([{logging, true}, {env, [{"NLS_LANG", "GERMAN_SWITZERLAND.AL32UTF8"}]}]).
{ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param).
OciSession = OciPort:get_session(Tns, User, Pswd).
SelStmt = OciSession:prep_sql("select * from lobs").
SelStmt:exec_stmt().
{{rows, Rows}, _} = SelStmt:fetch_rows(100).
SelStmt:lob(50713328, 1, 6).

f().
OciPort = oci_port:start_link([{logging, true}, {env, [{"NLS_LANG", "GERMAN_SWITZERLAND.AL32UTF8"}]}]).
Tns = <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=5437)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>.
Pswd= <<"regit">>.
User= <<"scott">>.
OciSession = OciPort:get_session(Tns, User, Pswd).
SelStmt = OciSession:prep_sql("select * from lobs").
SelStmt:exec_stmt().
{{rows, Rows}, _} = SelStmt:fetch_rows(100).
SelStmt:lob(51075816, 1, 20).

f(OciPort).
f(OciSession).
f(BindInsQryStr).
f(VarBindList).
f(BoundInsStmt).
f(SelStmt).
f(Cols).

f().
OciPort = oci_port:start_link([{logging, true}, {env, [{"NLS_LANG", "GERMAN_SWITZERLAND.AL32UTF8"}]}]).
{ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param).
[spawn(fun()-> OciSession = OciPort:get_session(Tns, User, Pswd), io:format(user, "~p. OciSession ~p~n", [I, OciSession]) end) || I <- lists:seq(1,2)].

OciSession = OciPort:get_session(Tns, User, Pswd).
SelStmt = OciSession:prep_sql("select * from erloci_test_1").
SelStmt:exec_stmt().
SelStmt:fetch_rows(100).

OciSession = OciPort:get_session(<<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=5437)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>, <<"scott">>, <<"regit">>).
OciPort:keep_alive(false).

f(SelStmt).
SelStmt = OciSession:prep_sql("select * from erloci_test_1").
SelStmt:exec_stmt().
SelStmt:fetch_rows(100).
SelStmt:close().

f(InsertStmt).
InsertStmt = OciSession:prep_sql(<<"insert into SCOTT.T2 (N, D, N72) values (:N, :D, :N72)">>).
InsertStmt:bind_vars([{<<":N">>,'SQLT_VNU'}, {<<":D">>,'SQLT_VNU'}, {<<":N72">>,'SQLT_VNU'}]).
InsertStmt:exec_stmt([{<<3,193,34,45>>,<<2,193,23>>,<<3,193,45,45>>},{<<3,193,4,34>>,<<2,193,23>>,<<2,193,24>>}]).
InsertStmt:close().


f(UpdStmt).
UpdStmt = OciSession:prep_sql(<<"update SCOTT.JUNK set SOMENUMBER = :SOMENUMBER, SOMEOTHER = :SOMEOTHER where PKEY = :PKEY and YETANOTHER = :YETANOTHER and HUGE = :HUGE and SCOTT.JUNK.ROWID = :IDENTIFIER">>).
UpdStmt:bind_vars([{<<":IDENTIFIER">>,'SQLT_STR'},{<<":PKEY">>,'SQLT_VNU'},{<<":SOMENUMBER">>,'SQLT_VNU'},{<<":SOMEOTHER">>,'SQLT_VNU'},{<<":YETANOTHER">>,'SQLT_VNU'},{<<":HUGE">>,'SQLT_VNU'}]).
UpdStmt:exec_stmt([{<<"AAAHNHAABAAALGRAAL">>,<<>>,<<3,194,2,37>>,<<3,192,89,74>>,<<>>,<<>>},{<<"AAAHNHAABAAALGRAAO">>,<<2,193,23>>,<<3,194,4,34>>,<<3,192,33,81>>,<<>>,<<1,128>>}]).
UpdStmt:close().

f(UpdStmt2).
UpdStmt2 = OciSession:prep_sql(<<"update SCOTT.JUNK set SOMENUMBER = :SOMENUMBER, SOMEOTHER = :SOMEOTHER where PKEY = :PKEY and SCOTT.JUNK.ROWID = :IDENTIFIER">>).
UpdStmt2:bind_vars([{<<":IDENTIFIER">>,'SQLT_STR'},{<<":PKEY">>,'SQLT_VNU'},{<<":SOMENUMBER">>,'SQLT_VNU'},{<<":SOMEOTHER">>,'SQLT_VNU'}]).
UpdStmt2:exec_stmt([{<<"AAAHNHAABAAALGRAAO">>,<<2,193,23>>,<<3,194,4,34>>,<<3,192,33,81>>}]).
UpdStmt2:exec_stmt([{<<"AAAHNHAABAAALGRAAL">>,<<>>,<<3,194,2,37>>,<<3,192,89,74>>}]).
UpdStmt2:close().


f(UpdStmt1).
UpdStmt1 = OciSession:prep_sql(<<"update SCOTT.JUNK set SOMENUMBER = :SOMENUMBER, SOMEOTHER = :SOMEOTHER where SCOTT.JUNK.ROWID = :IDENTIFIER">>).
UpdStmt1:bind_vars([{<<":IDENTIFIER">>,'SQLT_STR'},{<<":SOMENUMBER">>,'SQLT_VNU'},{<<":SOMEOTHER">>,'SQLT_VNU'}]).
UpdStmt1:exec_stmt([{<<"AAAHNHAABAAALGRAAO">>,<<3,194,4,34>>,<<3,192,33,81>>},{<<"AAAHNHAABAAALGRAAL">>,<<3,194,2,37>>,<<3,192,89,74>>}]).
UpdStmt1:close().



SelStmt:close().
f(SelStmt).
SelStmt = OciSession:prep_sql("select * from abc").
SelStmt:exec_stmt().
SelStmt:fetch_rows(100).

SelStmt:close().
f(SelStmt).
f(Cols).
SelStmt = OciSession:prep_sql("select 10.5 from dual").
{ok, Cols} = SelStmt:exec_stmt().
SelStmt:fetch_rows(2).

OciPort = oci_port:start_link([{logging, true}]).
{ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param).
OciSession = OciPort:get_session(Tns, User, Pswd).

SelStmt = OciSession:prep_sql(<<"select erloci_test_1.rowid, erloci_test_1.* from erloci_test_1">>).
SelStmt:exec_stmt().
SelStmt:fetch_rows(100).

BindUdpQStr = <<"update erloci_test_1 set pkey = :pkey, publisher = :publisher, rank = :rank, hero = :hero, reality = :reality, votes = :votes, createdate = :createdate, votes_first_rank =:votes_first_rank where erloci_test_1.rowid = :pri_rowid1">>.
VarBindList = [ {<<":pkey">>, 'SQLT_INT'}
              , {<<":publisher">>, 'SQLT_CHR'}
              , {<<":rank">>, 'SQLT_FLT'}
              , {<<":hero">>, 'SQLT_CHR'}
              , {<<":reality">>, 'SQLT_CHR'}
              , {<<":votes">>, 'SQLT_INT'}
              , {<<":createdate">>, 'SQLT_DAT'}
              , {<<":votes_first_rank">>, 'SQLT_INT'}
              , {<<":pri_rowid1">>, 'SQLT_STR'}
              ].

RowIDs = [<<"AAAJ4HAABAAALDpAAA">>,
 <<"AAAJ4HAABAAALDpAAB">>,
 <<"AAAJ4HAABAAALDpAAC">>,
 <<"AAAJ4HAABAAALDpAAD">>,
 <<"AAAJ4HAABAAALDpAAE">>].

BoundUpdStmt = OciSession:prep_sql(BindUdpQStr).
BoundUpdStmt:bind_vars(VarBindList).
BoundUpdStmt:exec_stmt(
        [{ I
         , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
         , I
         , list_to_binary(["_Hero_",integer_to_list(I),"_"])
         , list_to_binary(["_Reality_",integer_to_list(I),"_"])
         , I
         , oci_util:edatetime_to_ora(erlang:now())
         , I
         , Key
         } || {Key, I} <- lists:zip(RowIDs, lists:seq(1, length(RowIDs)))]).

BindInsQryStr = list_to_binary(["insert into erloci_test_1",
                                " (pkey,publisher,rank,hero,reality,votes,createdate,votes_first_rank) values (",
                                ":pkey",
                                ", :publisher",
                                ", :rank",
                                ", :hero",
                                ", :reality",
                                ", :votes",
                                ", :createdate"
                                ", :votes_first_rank)"]).
VarBindList = [ {<<":pkey">>, 'SQLT_INT'}
              , {<<":publisher">>, 'SQLT_CHR'}
              , {<<":rank">>, 'SQLT_FLT'}
              , {<<":hero">>, 'SQLT_CHR'}
              , {<<":reality">>, 'SQLT_CHR'}
              , {<<":votes">>, 'SQLT_INT'}
              , {<<":createdate">>, 'SQLT_DAT'}
              , {<<":votes_first_rank">>, 'SQLT_INT'}
              ].
BoundInsStmt = OciSession:prep_sql(BindInsQryStr).
BoundInsStmt:bind_vars(VarBindList).
BoundInsStmt:exec_stmt(
        [{ I
         , list_to_binary(["_publisher_",integer_to_list(I),"_"])
         , I+I/2
         , list_to_binary(["_hero_",integer_to_list(I),"_"])
         , list_to_binary(["_reality_",integer_to_list(I),"_"])
         , I
         , oci_util:edatetime_to_ora(erlang:now())
         , I
         } || I <- lists:seq(1, 10)]).


OciSession:describe(<<"erloci_test_1">>, 'OCI_PTYPE_TABLE').
OciSession:describe(<<"all_tables">>, 'OCI_PTYPE_VIEW').
OciSession:describe(<<"all_users">>, 'OCI_PTYPE_VIEW').
OciSession:describe(<<"all_tables_view">>, 'OCI_PTYPE_VIEW').

OciSession:close().
OciPort:close().










StmtInsert = OciSession:prep_sql(<<"insert into test_table(pkey,publisher,rank,hero,reality,votes,createdate,votes_first_rank) values (:pkey, :publisher, :rank, :hero, :reality, :votes, :createdate, :votes_first_rank)">>).
StmtInsert:bind_vars([ {<<":pkey">>, 'SQLT_INT'}
                   , {<<":rank">>, 'SQLT_FLT'}
                   , {<<":hero">>, 'SQLT_CHR'}
                   , {<<":reality">>, 'SQLT_CHR'}
                   , {<<":votes">>, 'SQLT_INT'}
                   , {<<":createdate">>, 'SQLT_DAT'}
                   , {<<":votes_first_rank">>, 'SQLT_INT'}
                   ]).
StmtInsert:exec_stmt([{ I
         , I+I/2
         , list_to_binary(["_hero_",integer_to_list(I),"_"])
         , list_to_binary(["_reality_",integer_to_list(I),"_"])
         , I
         , oci_util:edatetime_to_ora(erlang:now())
         , I
         } || I <- lists:seq(1, 10)]).




%%%%%%%%%%%%%%%%%%%%%%%% BIG TABLE %%%%%%%%%%%%%%%%%%%%%%%%%%
Os:close().
OciPort:close().
f(OciPort).
f(Os).
{OciPort, Os} = oci_test:bigtab_setup().
ok = oci_test:bigtab_load(Os, 10000).
oci_test:bigtab_access(Os, 100).

oci_test:bigtable_test(50000, 1000).

create directory "/u01/app/oracle/dbtestfiles" as '/u01/app/oracle/dbtestfiles';
create directory "/dbfast/oracle" as '/dbfast/oracle';

insert into lobs values(to_clob('clobd0'), hextoraw('453d7a30'), to_nclob('nclobd0'), bfilename('/u01/app/oracle/dbtestfiles', 'test1.bin'));
insert into lobs values(to_clob('clobd1'), hextoraw('453d7a31'), to_nclob('nclobd1'), bfilename('/u01/app/oracle/dbtestfiles', 'test2.bin'));
insert into lobs values(to_clob('clobd2'), hextoraw('453d7a32'), to_nclob('nclobd2'), bfilename('/u01/app/oracle/dbtestfiles', 'test3.bin'));
insert into lobs values(to_clob('clobd3'), hextoraw('453d7a33'), to_nclob('nclobd3'), bfilename('/u01/app/oracle/dbtestfiles', 'test4.bin'));
insert into lobs values(to_clob('clobd4'), hextoraw('453d7a34'), to_nclob('nclobd4'), bfilename('/u01/app/oracle/dbtestfiles', 'test5.bin'));
insert into lobs values(to_clob('clobd5'), hextoraw('453d7a35'), to_nclob('nclobd5'), bfilename('/u01/app/oracle/dbtestfiles', 'test6.bin'));
insert into lobs values(to_clob('clobd6'), hextoraw('453d7a36'), to_nclob('nclobd6'), bfilename('/dbfast/oracle', 'test7.bin'));
insert into lobs values(to_clob('clobd7'), hextoraw('453d7a37'), to_nclob('nclobd7'), bfilename('/dbfast/oracle', 'test8.bin'));
insert into lobs values(to_clob('clobd8'), hextoraw('453d7a38'), to_nclob('nclobd8'), bfilename('/dbfast/oracle', 'test9.bin'));


insert into lobs values(to_clob('ABCD1234ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD1234444444444444444444444444444444444444444444444444'), hextoraw('0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F0102030405060708090A0B0C0D0E0F'), to_nclob('ABCD1234ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD123ABCD1234444444444444444444444444444444444444444444444444nclobd8'), bfilename('/dbfast/oracle', 'test9.bin'));

set pagesize 1000
set linesize 1000
column clobd format a10
column blobd format a10
column nclobd format a10
column bfiled format a60
column longd format a10

select * from datatypes;

set pagesize 1000
set linesize 1000
column rawd format a10
column longd format a10
column longrawd format a10


/////////////////////////////////////////////////////////////////

create or replace procedure mysum(p_first in number, p_second in number, p_result out number)
is
begin
p_result := p_first + p_second;
end;
/

create or replace procedure myproc(p_first in number, p_second in out varchar2, p_result out number)
is
begin
    p_result := p_first + to_number(p_second);
    p_second := 'The sum is ' || to_char(p_result);
end;
/

variable s number;
variable t varchar2(30);
exec :s := 1;
exec :t := '5';

begin myproc(1,:t,:s); end;
/

call myproc(1,:t,:s);

declare
begin
myproc(1,:t,:s);
end;
/

print :s;
print :t;

///////////////////

application:start(erloci).

f().
OciPort = oci_port:start_link([{logging, true}, {env, []}]).
Tns = <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=192.168.1.69)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=SBS0.k2informatics.ch)))">>.
User= <<"sbs0">>.
Pswd= <<"sbs0sbs0_4dev">>.
{ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param).
OciSession = OciPort:get_session(Tns, User, Pswd).

f(SelStmt).
SelStmt = OciSession:prep_sql("SELECT queue, user_data  FROM aq$sisnot_aq_table").
SelStmt = OciSession:prep_sql("SELECT *  FROM contacts").
SelStmt:exec_stmt().
SelStmt:fetch_rows(100).
SelStmt:close().

f(SelStmt).
SelStmt = OciSession:prep_sql("SELECT col FROM duplicate").
SelStmt:exec_stmt().
SelStmt:fetch_rows(100).
SelStmt:close().


OciPort:close().

drop table datetime;
create table datetime (
    name varchar(30),
    dat DATE DEFAULT (sysdate),
    ts TIMESTAMP DEFAULT (systimestamp),
    tstz timestamp with time zone DEFAULT (systimestamp),
    tsltz timestamp with local time zone DEFAULT (systimestamp),
    iym INTERVAL YEAR(3) TO MONTH default '234-2',
    ids INTERVAL DAY TO SECOND(3) default '4 5:12:10.222');

insert into datetime(name) values ('test1');

select dump(dat), dat from datetime;
select dump(ts), ts from datetime;
select dump(tstz), tstz from datetime;
select dump(tsltz), tsltz from datetime;
select dump(iym), iym from datetime;
select dump(ids), ids from datetime;


application:start(erloci).

f().
OciPort = oci_port:start_link([{logging, true}, {env, []}]).
Tns = <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=5437)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>.
Pswd= <<"regit">>.
User= <<"scott">>.
OciSession = OciPort:get_session(Tns, User, Pswd).

InsStmt = OciSession:prep_sql(<<"insert into datetime (name) values ('test10'); insert into datetime (name) values ('test11')">>).
InsStmt:exec_stmt().


% All
f(BoundAllInsStmt).
BoundAllInsStmt = OciSession:prep_sql(<<"insert into datetime (name, dat, ts, tstz, tsltz, iym, ids)
 values (:name, :dat, :ts, :tstz, :tsltz, :iym, :ids)">>).

BoundAllInsStmt:bind_vars(
[ {<<":name">>, 'SQLT_CHR'}
, {<<":dat">>, 'SQLT_DAT'}
, {<<":ts">>, 'SQLT_TIMESTAMP'}
, {<<":tstz">>, 'SQLT_TIMESTAMP_TZ'}
, {<<":tsltz">>, 'SQLT_TIMESTAMP_LTZ'}
, {<<":iym">>, 'SQLT_INTERVAL_YM'}
, {<<":ids">>, 'SQLT_INTERVAL_DS'}]).

BoundAllInsStmt:exec_stmt(
[{<<"test1_6">>
, oci_util:to_dts({{2014+1,8+1,9+1}, {10+1,11+1,12+1}})
, oci_util:to_dts({{2014+1,8+1,9+1}, {10+1,11+1,12+1}, 300+1})
, oci_util:to_dts({{2014+1,8+1,9+1}, {10+1,11+1,12+1}, 300+1, {2+1,3+1}})
, oci_util:to_dts({{2014+1,8+1,9+1}, {10+1,11+1,12+1}, 300+1})
, oci_util:to_intv({243,2})
, oci_util:to_intv({5,4,11,11,300+1})}]).


%%% One by one
f(BoundAllInsStmt).
BoundAllInsStmt = OciSession:prep_sql(<<"insert into datetime (name, ts)
 values (:name, :ts)">>).

BoundAllInsStmt:bind_vars(
[ {<<":name">>, 'SQLT_CHR'}
, {<<":ts">>, 'SQLT_TIMESTAMP'}
]).

BoundAllInsStmt:exec_stmt(
[{
  <<"test1_3">>
, oci_util:to_dts({{2014+1,8+1,9+1}, {10+1,11+1,12+1}, 300+1})
}]).

BoundAllInsStmt:close().


