-module(oci_util).

-export([ edatetime_to_ora/1
        , oradate_to_str/1
        , oranumber_decode/1
        , from_ts/1]).

edatetime_to_ora({Meg,Mcr,Mil} = Now)
    when is_integer(Meg)
    andalso is_integer(Mcr)
    andalso is_integer(Mil) ->
    edatetime_to_ora(calendar:now_to_datetime(Now));
edatetime_to_ora({{FullYear,Month,Day},{Hour,Minute,Second}}) ->
    Century = (FullYear div 100) + 100,
    Year = (FullYear rem 100) + 100,
    << Century:8, Year:8, Month:8, Day:8, Hour:8, Minute:8, Second:8 >>.

oradate_to_str(<<Year:16, Month:8, Day:8, Hour:8, Min:8, Sec:8, _/binary>>) ->
    lists:flatten(io_lib:format(
        "~4..0B.~2..0B.~2..0B ~2..0B:~2..0B:~2..0B"
        , [Year, Month, Day, Hour, Min, Sec])).

oranumber_decode(<<0:8, _/binary>>) -> {0, 0};
oranumber_decode(<<1:8, _/binary>>) -> {0, 0};
oranumber_decode(<<Length:8, 1:1, OraExp:7, Rest/binary>>) -> % positive numbers
    Exponent = OraExp - 65,
    MLength = Length - 1,
    <<OraMantissa:MLength/binary, _/binary>> = Rest,
    ListOraMant = binary_to_list(OraMantissa),
    ListMantissa = lists:flatten([io_lib:format("~2.10.0B", [DD-1]) || DD <- ListOraMant]),
    Mantissa = list_to_integer(ListMantissa),
    LengthMant = length(ListMantissa),
    oraexp_to_imem_prec(Mantissa, Exponent, LengthMant);
oranumber_decode(<<Length:8, 0:1, OraExp:7, Rest/binary>>) -> % negative numbers
    Exponent = 62 - OraExp,
    MLength = Length - 2,
    <<OraMantissa:MLength/binary, 102, _/binary>> = Rest,
    ListOraMant = binary_to_list(OraMantissa),
    ListMantissa = lists:flatten([io_lib:format("~2.10.0B", [101-DD]) || DD <- ListOraMant]),
    Mantissa = -1 * list_to_integer(ListMantissa),
    LengthMant = length(ListMantissa),
    oraexp_to_imem_prec(Mantissa, Exponent, LengthMant);
oranumber_decode(_) -> {error, <<"invalid oracle number">>}.

oraexp_to_imem_prec(Mantissa, Exponent, LengthMant) ->
    oraexp_to_imem_prec(Mantissa, Exponent, LengthMant, LengthMant rem 2, Mantissa rem 10).

oraexp_to_imem_prec(Mantissa,Exponent,LengthMant,RemLength,0) ->
    {Mantissa div 10, (Exponent*-2) + LengthMant-3 + RemLength};
oraexp_to_imem_prec(Mantissa,Exponent,LengthMant,RemLength,_) ->
    {Mantissa, (Exponent*-2) + LengthMant-2 + RemLength}.

from_ts(
    <<C:1/integer-unit:8, Y:1/integer-unit:8, M:1/integer-unit:8, D:1/integer-unit:8
    , H:1/integer-unit:8, Min:1/integer-unit:8, S:1/integer-unit:8
    , Ns:4/little-unsigned-integer-unit:8, Rest/binary>>
    ) ->
    {Date, Time, Ns} = from_ts({C, Y, M, D, H, Min, S, Ns}),
    case Rest of
        <<>> -> {Date, Time, Ns};
        <<TzH:1/integer-unit:8
        , TzM:1/integer-unit:8>> ->
            {Date, Time, Ns
            , {TzH - 20 % Hour of TimeZone
            , TzM - 60} % Minute of TimeZone
            }
    end;
from_ts({C, Y, M, D, H, Min, S, Ns}) ->
    {{ (C - 100) * 100 + (Y - 100) % Year
     , M - 1                       % Month
     , D - 1}                      % Day
    ,{ H - 1                       % Hour
     , Min - 1                     % Minute
     , S- 1}                       % Second
    , Ns                           % Nano second in second 
    }.
