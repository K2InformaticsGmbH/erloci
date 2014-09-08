-module(oci_util).

-export([ edatetime_to_ora/1
        , oradate_to_str/1
        , oranumber_decode/1
        , from_dts/1
        , from_intv/1
        , to_dts/1
        , to_intv/1
        ]).

-type year()        :: pos_integer().
-type month()       :: 1..12.
-type day()         :: 1..31.
-type hour()        :: 0..23.
-type minute()      :: 0..59.
-type second()      :: 0..59.
-type nanosecond()  :: 0..999999999.
-type tzhour()      :: -12..14.
-type tzminute()    :: minute().

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

-spec from_dts(Date | TimeStamp | TimeStampWithZone) ->
        {{year(),month(),day()}, {hour(),minute(),second()}}
    |   {{year(),month(),day()}, {hour(),minute(),second()}
         , nanosecond()}
    |   {{year(),month(),day()}, {hour(),minute(),second()}
         , nanosecond()
         , {tzhour(),tzminute()}}
      when
      Date              :: <<_:56>>,    %  7 bytes
      TimeStamp         :: <<_:88>>,    % 11 bytes
      TimeStampWithZone :: <<_:104>>.   % 13 bytes
from_dts( % 7 bytes
    <<C:1/integer-unit:8, Y:1/integer-unit:8, M:1/integer-unit:8, D:1/integer-unit:8
    , H:1/integer-unit:8, Min:1/integer-unit:8, S:1/integer-unit:8>>
    ) ->
    from_dts({C, Y, M, D, H, Min, S});
from_dts( % 11 bytes
    <<C:1/integer-unit:8, Y:1/integer-unit:8, M:1/integer-unit:8, D:1/integer-unit:8
    , H:1/integer-unit:8, Min:1/integer-unit:8, S:1/integer-unit:8
    , Ns:4/little-unsigned-integer-unit:8>>
    ) ->
    {Date, Time} = from_dts({C, Y, M, D, H, Min, S}),
    {Date, Time
     , Ns};         % Nano Second
from_dts( % 13 bytes
    <<C:1/integer-unit:8, Y:1/integer-unit:8, M:1/integer-unit:8, D:1/integer-unit:8
    , H:1/integer-unit:8, Min:1/integer-unit:8, S:1/integer-unit:8
    , Ns:4/little-unsigned-integer-unit:8, TzH:1/integer-unit:8, TzM:1/integer-unit:8>>
    ) ->
    {Date, Time} = from_dts({C, Y, M, D, H, Min, S}),
    {Date, Time
     , Ns           % Nano Second
     , {TzH - 20    % Hour of TimeZone
        , TzM - 60} % Minute of TimeZone
    };
from_dts({C, Y, M, D, H, Min, S}) ->
    {{ (C - 100) * 100 + (Y - 100)  % Year
     , M                            % Month
     , D}                           % Day
    ,{ H - 1                        % Hour
     , Min - 1                      % Minute
     , S- 1}                        % Second
    }.

-spec from_intv(YearToMonth | DayToSecond) ->
        {integer(),integer()}
    |   {integer(),integer(),hour(),minute(),second(),nanosecond()}
      when
      YearToMonth   :: <<_:40>>,    %  5 bytes
      DayToSecond   :: <<_:88>>.    % 11 bytes
from_intv(<<Y:4/integer-unit:8, M:1/integer-unit:8>>) ->
    {Y - 2147483648, M - 60};
from_intv(<<D:4/integer-unit:8, H:1/integer-unit:8
          , M:1/integer-unit:8, S:1/integer-unit:8
          , Ns:4/little-unsigned-integer-unit:8>>) ->
    {D - 2147483648, H - 60, M - 60, S - 60, Ns}.

-spec to_dts( {{year(),month(),day()}
               , {hour(),minute(),second()}}
            | {{year(),month(),day()}
               , {hour(),minute(),second()}
               , nanosecond()}
            | {{year(),month(),day()}
               , {hour(),minute(),second()}
               , nanosecond()
               , {tzhour(),tzminute()}}) ->
    Date | TimeStamp | TimeStampWithZone
      when
      Date              :: <<_:56>>,    %  7 bytes
      TimeStamp         :: <<_:88>>,    % 11 bytes
      TimeStampWithZone :: <<_:104>>.   % 13 bytes
to_dts({{Year, Month, Day}, {Hour, Minute, Second}, Ns, {TimeZoneHour, TimeZoneMinute}}) ->
    DateTimeNsBin = to_dts({{Year, Month, Day}, {Hour, Minute, Second}, Ns}),
    TzH = TimeZoneHour + 20,
    TzM = TimeZoneMinute + 60,
    % 13 bytes
    <<DateTimeNsBin/binary, TzH:1/integer-unit:8, TzM:1/integer-unit:8>>;
to_dts({{Year, Month, Day}, {Hour, Minute, Second}, Ns}) ->
    DateTimeBin = to_dts({{Year, Month, Day}, {Hour, Minute, Second}}),
    % 11 bytes
    <<DateTimeBin/binary, Ns:4/little-unsigned-integer-unit:8>>;
to_dts({{Year, Month, Day}, {Hour, Minute, Second}}) ->
    C = (Year + 10100) div 100 - 1,
    Y = (Year + 10100) rem 100 + 100,
    M = Month,
    D = Day,
    H = Hour + 1,
    Min = Minute + 1,
    S = Second + 1,
    % 7 bytes
    <<C:1/integer-unit:8, Y:1/integer-unit:8, M:1/integer-unit:8, D:1/integer-unit:8
    , H:1/integer-unit:8, Min:1/integer-unit:8, S:1/integer-unit:8>>.

-spec to_intv({integer(),integer()}
              | {integer(),integer()
                 ,hour(),minute(),second()
                 ,nanosecond()}) ->
    YearToMonth | DayToSecond
      when
      YearToMonth   :: <<_:40>>,    %  5 bytes
      DayToSecond   :: <<_:88>>.    % 11 bytes
to_intv({Year, Month}) ->
    Y = Year + 2147483648,
    M = Month + 60,
    <<Y:4/integer-unit:8, M:1/integer-unit:8>>;
to_intv({Day, Hour, Minute, Second, Ns}) ->
    D = Day + 2147483648,
    H = Hour + 60,
    M = Minute + 60,
    S = Second + 60,
    <<D:4/integer-unit:8
      , H:1/integer-unit:8, M:1/integer-unit:8, S:1/integer-unit:8
      , Ns:4/little-unsigned-integer-unit:8>>.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

dts_conv_test_() ->
    {inparallel
     , [{P, fun() ->
                    ?assertEqual(O, from_dts(P)),
                    ?assertEqual(P, to_dts(O)),
                    ok
            end}
        || {P,O} <-
           [
              {<<120,114,9,7,11,54,4>>,                     {{2014,9,7},{10,53,3}}}
            , {<<120,114,9,8,13,11,40,0,110,62,168>>,       {{2014,9,8},{12,10,39},2822663680}}
            , {<<120,114,9,8,11,11,40,0,110,62,168,22,60>>, {{2014,9,8},{10,10,39},2822663680,{2,0}}}
            , {<<120,114,9,8,11,11,40,0,110,62,168>>,       {{2014,9,8},{10,10,39},2822663680}}

            , {<<120,114,9,8,13,11,40,0,110,62,168>>,       {{2014,9,8},{12,10,39},2822663680}}
            , {<<120,114,9,8,11,11,40,0,110,62,168,22,60>>, {{2014,9,8},{10,10,39},2822663680,{2,0}}}
            , {<<120,114,9,8,11,11,40,0,110,62,168>>,       {{2014,9,8},{10,10,39},2822663680}}
           ]
       ]
    }.

intv_conv_test_() ->
    {inparallel
     , [{P, fun() ->
                    ?assertEqual(O, from_intv(P)),
                    ?assertEqual(P, to_intv(O)),
                    ok
            end}
        || {P,O} <-
           [
              {<<128,0,0,234,62>>,                      {234,2}}
            , {<<128,0,0,4,65,72,70,141,59,115,128>>,   {4,5,12,10,2155035533}}            
           ]
       ]
    }.

-endif.
