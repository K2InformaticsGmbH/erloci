-module(k2_test).
-include("oci.hrl").

-define(NowMs, (fun() -> {M,S,Ms} = erlang:now(), ((M*1000000 + S)*1000000) + Ms end)()).

-export([ run/2 ]).

run(Threads, InsertCount) when is_integer(Threads), is_integer(InsertCount) ->
    OciSession = connect_db(),
    This = self(),
    [(fun(Idx) ->
        Table = "erloci_table_"++Idx,
        try
            create_table(OciSession, Table),            
            spawn(fun() -> insert_select(OciSession, Table, InsertCount, This) end)
        catch
            Class:Reason -> oci_logger:log(lists:flatten(io_lib:format("~p:~p~n", [Class,Reason])))
        end
    end)(integer_to_list(I))
    || I <- lists:seq(1,Threads)],
    receive_all(Threads).

receive_all(Count) -> receive_all(Count, []).
receive_all(0, Acc) ->
    [(fun(Table, InsertCount, InsertTime, SelectCount, SelectTime) ->
        InsRate = erlang:trunc(InsertCount / InsertTime),
        SelRate = erlang:trunc(SelectCount / SelectTime),
        io:format(user, "~p insert ~p, ~p sec, ~p rows/sec    select ~p, ~p sec, ~p rows/sec~n", [Table,InsertCount, InsertTime, InsRate,SelectCount, SelectTime, SelRate])
    end)(T, Ic, It, Sc, St)
    || {T, Ic, It, Sc, St} <- Acc];
receive_all(Count, Acc) ->
    receive
        {T, Ic, It, Sc, St} ->
            receive_all(Count-1, [{T, Ic, It, Sc, St}|Acc])
    after
        (100*1000) ->
            io:format(user, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ~p~n", [timeout]),
            receive_all(0, Acc)
    end.

connect_db() ->
    OciPort = oci_port:start_link([{logging, true}]),
    OciPool = OciPort:create_sess_pool(
                <<"(DESCRIPTION=
                    (ADDRESS_LIST=
                        (ADDRESS=(PROTOCOL=tcp)
                            (HOST=80.67.144.206)
                            (PORT=1521)
                        )
                    )
                    (CONNECT_DATA=(SERVICE_NAME=XE))
                )">>,
                <<"bikram">>,
                <<"abcd123">>,
                <<>>),
    %OciPool = OciPort:create_sess_pool(
    %            <<"(DESCRIPTION=
    %                (ADDRESS=(PROTOCOL=tcp)
    %                    (HOST=192.168.1.69)
    %                    (PORT=1521)
    %                )
    %                (CONNECT_DATA=(SERVICE_NAME=SBS0.k2informatics.ch))
    %            )">>,
    %            <<"SBS0">>,
    %            <<"sbs0sbs0_4dev">>,
    %            <<>>),
    throw_if_error(undefined, OciPool, "pool creation failed"),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI Pool ~p~n", [OciPool]))),

    OciSession = OciPort:get_session(),
    throw_if_error(undefined, OciSession, "get session failed"),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI Session ~p~n", [OciSession]))),
    OciSession.

create_table(OciSession, Table) ->
    Res = OciSession:exec_sql(list_to_binary(["drop table ",Table]), []),
    print_if_error(Res, "drop failed"),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI drop ~p~n", [Res]))),
    Res0 = OciSession:exec_sql(list_to_binary(["create table ",Table,"(pkey number,
                                       publisher varchar2(100),
                                       rank number,
                                       hero varchar2(100),
                                       real varchar2(100),
                                       votes number,
                                       created date default sysdate,
                                       votes_first_rank number)"]), []),
    throw_if_error(undefined, Res0, "create "++Table++" failed"),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI create ~p~n", [Res0]))).

insert_select(OciSession, Table, InsertCount, Parent) ->
    try
        InsertStart = ?NowMs,
        [(fun(I) ->
            Qry = erlang:list_to_binary([
                    "insert into ", Table, " (pkey,publisher,rank,hero,real,votes,votes_first_rank) values (",
                    I,
                    ", 'publisher"++I++"',",
                    I,
                    ", 'hero"++I++"'",
                    ", 'real"++I++"',",
                    I,
                    ",",
                    I,
                    ")"]),
            oci_logger:log(lists:flatten(io_lib:format("_[~p]_ ~p~n", [Table,Qry]))),
            Res = OciSession:exec_sql(Qry, []),
            throw_if_error(Parent, Res, "insert "++Table++" failed"),
            if {executed, no_ret} =/= Res -> oci_logger:log(lists:flatten(io_lib:format("_[~p]_ ~p~n", [Table,Res]))); true -> ok end
          end)(integer_to_list(Idx))
        || Idx <- lists:seq(1, InsertCount)],
        InsertEnd = ?NowMs,
        {Statement, {cols, Cols}} = OciSession:exec_sql(list_to_binary(["select * from ", Table]), []),
        throw_if_error(Parent, Statement, "select "++Table++" failed"),
        oci_logger:log(lists:flatten(io_lib:format("_[~p]_ statement ~p~n", [Table,Statement]))),
        oci_logger:log(lists:flatten(io_lib:format("_[~p]_ columns ~p~n", [Table,Cols]))),
        {{rows, Rows}, _} = Statement:get_rows(100),
        oci_logger:log(lists:flatten(io_lib:format("...[~p]... OCI select rows ~p~n", [Table,Rows]))),
        SelectEnd = ?NowMs,
        InsertTime = (InsertEnd - InsertStart)/1000000,
        SelectTime = (SelectEnd - InsertEnd)/1000000,
        Parent ! {Table, InsertCount, InsertTime, length(Rows), SelectTime}
    catch
        Class:Reason ->
            if is_pid(Parent) -> Parent ! {{Table,Class,Reason}, 0, 1, 0, 1}; true -> ok end,
            oci_logger:log(lists:flatten(io_lib:format(" ~p:~p~n", [Class,Reason])))
    end.

print_if_error({error, Error}, Msg) -> oci_logger:log(lists:flatten(io_lib:format("___________----- continue after ~p ~p~n", [Msg,Error])));
print_if_error(_, _) -> ok.

throw_if_error(Parent, {error, Error}, Msg) ->
    if is_pid(Parent) -> Parent ! {{Msg, Error}, 0, 1, 0, 1}; true -> ok end,
    throw({Msg, Error});
throw_if_error(_,_,_) -> ok.
