-include("oci.hrl").

-record(stmtCol,                                    %% simplified column map for client
                  { tag = undefined                 ::any()
                  , alias                           ::binary()          %% column name or expression
                  , type = term                     ::atom()
                  , len                             ::integer()
                  , prec = 0                        ::integer()
                  , readonly = false                ::true|false
                  }                  
       ).

-record(stmtResult,                                 %% result record for exec function call
                  { rowCount = 0                    %% RowCount
                  , stmtRef = undefined             %% id needed for fetching
                  , stmtCols = undefined            ::list(#stmtCol{})  %% simplified column map of main statement
                  , rowFun  = undefined             ::fun()             %% rendering fun for row {key rec} -> [ResultValues]
                  , sortFun = undefined             ::fun()             %% rendering fun for sorting {key rec} -> SortColumn
                  , sortSpec = []                   ::list()
                  }
       ).

-define(TYPEMAP, [
                    %-----------------------------------------------------------
                    % type      db to erlang                erlang to db
                    %-----------------------------------------------------------
                    {number,
                                fun(R)->
                                    binary_to_integer(R)
                                end,
                                                            fun(R)->
                                                                integer_to_binary(R)
                                                            end
                    },
                    {string,
                                fun(R)->
                                    binary_to_list(R)
                                end,
                                                            fun(R)->
                                                                list_to_binary(R)
                                                            end
                    },
                    {integer,
                                fun(R)->
                                    binary_to_integer(R)
                                end,
                                                            fun(R)->
                                                                integer_to_binary(R)
                                                            end
                    },
                    {date,
                                fun(R)->
                                    R
                                end,
                                                            fun(R)->
                                                                R
                                                            end
                    },
                    {double,
                                fun(R)->
                                    R
                                end,
                                                            fun(R)->
                                                                R
                                                            end
                    },
                    {timestamp,
                                fun(R)->
                                    R
                                end,
                                                            fun(R)->
                                                                R
                                                            end
                    },
                    {interval,
                                fun(R)->
                                    R
                                end,
                                                            fun(R)->
                                                                R
                                                            end
                    },
                    {undefined,
                                fun(R)->
                                    R
                                end,
                                                            fun(R)->
                                                                R
                                                            end
                    }
                    %-----------------------------------------------------------
]).
