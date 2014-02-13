%-ifdef(TEST).


-define(ELog(__F), ?ELog(__F,[])).
-define(ELog(__Fmt,__Args),
(fun(__F,__A) ->
    {_,{__H,__Min,__S}} = calendar:now_to_datetime(erlang:now()),
    io:format(user, "~2..0B:~2..0B:~2..0B [~p:~p] "++__F++"~n", [__H,__Min,__S,?MODULE,?LINE | __A])
end)(__Fmt,__Args)).

-define(TESTTABLE, "erloci_test_1").
-define(TESTFUNCTION, "ERLOCI_TEST_FUNCTION").
-define(DROP,   <<"drop table "?TESTTABLE>>).
-define(CREATE, <<"create table "?TESTTABLE" (pkey integer,"
                  "publisher varchar2(30),"
                  "rank float,"
                  "hero varchar2(30),"
                  "reality varchar2(30),"
                  "votes number(1,-10),"
                  "createdate date default sysdate,"
                  "chapters int,"
                  "votes_first_rank number)">>).
-define(INSERT, <<"insert into "?TESTTABLE
                  " (pkey,publisher,rank,hero,reality,votes,createdate,votes_first_rank) values ("
                  ":pkey"
                  ", :publisher"
                  ", :rank"
                  ", :hero"
                  ", :reality"
                  ", :votes"
                  ", :createdate"
                  ", :votes_first_rank)">>).
-define(SELECT_WITH_ROWID, <<"select "?TESTTABLE".rowid, "?TESTTABLE".* from "?TESTTABLE>>).
-define(SELECT_ROWID_ASC, <<"select rowid from ", ?TESTTABLE, " order by pkey">>).
-define(SELECT_ROWID_DESC, <<"select rowid from ", ?TESTTABLE, " order by pkey desc">>).
-define(BIND_LIST, [ {<<":pkey">>, 'SQLT_INT'}
                   , {<<":publisher">>, 'SQLT_CHR'}
                   , {<<":rank">>, 'SQLT_FLT'}
                   , {<<":hero">>, 'SQLT_CHR'}
                   , {<<":reality">>, 'SQLT_CHR'}
                   , {<<":votes">>, 'SQLT_INT'}
                   , {<<":createdate">>, 'SQLT_DAT'}
                   , {<<":votes_first_rank">>, 'SQLT_INT'}
                   ]).
-define(UPDATE, <<"update "?TESTTABLE" set "
                  "pkey = :pkey"
                  ", publisher = :publisher"
                  ", rank = :rank"
                  ", hero = :hero"
                  ", reality = :reality"
                  ", votes = :votes"
                  ", createdate = :createdate"
                  ", votes_first_rank = :votes_first_rank"
                  " where "?TESTTABLE".rowid = :pri_rowid1">>).
-define(UPDATE_BIND_LIST, [ {<<":pkey">>, 'SQLT_INT'}
                          , {<<":publisher">>, 'SQLT_CHR'}
                          , {<<":rank">>, 'SQLT_FLT'}
                          , {<<":hero">>, 'SQLT_CHR'}
                          , {<<":reality">>, 'SQLT_CHR'}
                          , {<<":votes">>, 'SQLT_STR'}
                          , {<<":createdate">>, 'SQLT_DAT'}
                          , {<<":votes_first_rank">>, 'SQLT_INT'}
                          , {<<":pri_rowid1">>, 'SQLT_STR'}
                          ]).

%-endif.
