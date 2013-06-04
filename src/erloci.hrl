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
