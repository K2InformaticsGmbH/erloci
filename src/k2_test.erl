-module(k2_test).
-include("oci.hrl").

-define(NowMs, (fun() -> {M,S,Ms} = erlang:now(), ((M*1000000 + S)*1000000) + Ms end)()).

-export([ run/2 ]).

run(Threads, InsertCount) when is_integer(Threads), is_integer(InsertCount) ->
    OciSession = connect_db(),
    [(fun(Idx) ->
        Table = "erloci_table_"++Idx,
        try
            create_table(OciSession, Table),
            spawn(fun() -> insert_select(OciSession, Table, InsertCount) end)
        catch
            Class:Reason -> oci_logger:log(lists:flatten(io_lib:format("~p:~p~n", [Class,Reason])))
        end
    end)(integer_to_list(I))
    || I <- lists:seq(1,Threads)].

connect_db() ->
    OciPort = oci_port:start_link([{logging, true}]),
    OciPool = OciPort:create_sess_pool(
                <<"(DESCRIPTION=
                    (ADDRESS=(PROTOCOL=tcp)
                        (HOST=192.168.1.69)
                        (PORT=1521)
                    )
                    (CONNECT_DATA=(SERVICE_NAME=SBS0.k2informatics.ch))
                )">>,
                <<"SBS0">>,
                <<"sbs0sbs0_4dev">>,
                <<>>),
    throw_if_error(OciPool, "pool creation failed"),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI Pool ~p~n", [OciPool]))),

    OciSession = OciPort:get_session(),
    throw_if_error(OciSession, "get session failed"),
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
                                       votes_first_rank number)"]), []),
    throw_if_error(Res0, "create failed"),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI create ~p~n", [Res0]))).

insert_select(OciSession, Table, InsertCount) ->
    try
        InsertStart = ?NowMs,
        [(fun(I) ->
            Qry = erlang:list_to_binary([
                    "insert into ", Table, " values (",
                    I,
                    ", 'publisher"++I++"',",
                    I,
                    ", 'hero"++I++"'",
                    ", 'real"++I++"',",
                    I,
                    ",",
                    I,
                    ")"]),
            oci_logger:log(lists:flatten(io_lib:format("...[~p]... OCI insert sql ~p~n", [Table,Qry]))),
            Res = OciSession:exec_sql(Qry, []),
            throw_if_error(Res, "insert failed"),
            oci_logger:log(lists:flatten(io_lib:format("...[~p]... OCI insert ~p~n", [Table,Res])))
          end)(integer_to_list(Idx))
        || Idx <- lists:seq(1, InsertCount)],
        InsertEnd = ?NowMs,
        {Statement, {cols, Cols}} = OciSession:exec_sql(list_to_binary(["select * from ", Table]), []),
        throw_if_error(Statement, "select failed"),
        oci_logger:log(lists:flatten(io_lib:format("...[~p]... OCI select statement ~p~n", [Table,Statement]))),
        oci_logger:log(lists:flatten(io_lib:format("...[~p]... OCI select columns ~p~n", [Table,Cols]))),
        {{rows, Rows}, _} = Statement:get_rows(100),
        %oci_logger:log(lists:flatten(io_lib:format("...[~p]... OCI select rows ~p~n", [Table,Rows]))),
        SelectEnd = ?NowMs,
        InsertTime = (InsertEnd - InsertStart)/1000000,
        oci_logger:log(lists:flatten(io_lib:format("...[~p]... OCI inserted ~p rows in ~p sec @ ~p rows/sec~n", [Table,InsertCount, InsertTime, erlang:trunc(InsertCount / InsertTime)]))),
        SelectTime = (SelectEnd - InsertEnd)/1000000,
        oci_logger:log(lists:flatten(io_lib:format("...[~p]... OCI selected ~p rows in ~p sec @ ~p rows/sec~n", [Table,length(Rows), SelectTime, erlang:trunc(length(Rows) / SelectTime)])))
    catch
        Class:Reason -> oci_logger:log(lists:flatten(io_lib:format(" ~p:~p~n", [Class,Reason])))
    end.

print_if_error({error, Error}, Msg) -> oci_logger:log(lists:flatten(io_lib:format("___________----- continue after ~p ~p~n", [Msg,Error])));
print_if_error(_, _) -> ok.

throw_if_error({error, Error}, Msg) -> throw({Msg, Error});
throw_if_error(_, _) -> ok.
