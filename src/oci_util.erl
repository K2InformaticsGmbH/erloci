-module(oci_util).

-export([ edatetime_to_ora/1
        , oradate_to_str/1
        , oranumber_decode/1
        , from_dts/1
        , from_intv/1
        , to_dts/1
        , to_intv/1
        , from_num/1
        %, to_num/1
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

%% The high-order bit of the exponent byte is the sign bit;
%% it is set for positive numbers and it is cleared for
%% negative numbers. The lower 7 bits represent the exponent,
%% which is a base-100 digit with an offset of 65.

-ifdef(DEBUG).
-define(TO_STR(_M, _E),
(fun() ->
         io:format(user, "M : ~p, E : ~p~n", [_M, _E]),
         Ms = ins_dp(_M, _E),
         io:format(user, "Ms : ~p~n", [Ms]),
         FM = lists:flatten(Ms),
         io:format(user, "FM : ~p~n", [FM]),
         SM = strip(FM),
         io:format(user, "SM : ~p~n", [SM]),
         SM
end)()).
-else.
-define(TO_STR(_M, _E), strip(lists:flatten(ins_dp(_M, _E)))).
-endif.
-spec from_num(binary()) -> list().
from_num(<< Len:8/integer, 1:1/integer-unit:1, E:7/integer-unit:1, Rest/bytes >>) ->
    MantissaLen = Len - 1,
    if MantissaLen > 0 ->
           << MantissaBytes:MantissaLen/bytes, _/bytes >> = Rest,
           % Each mantissa byte is a base-100 digit, in the range 1..100. For positive numbers,
           % the digit has 1 added to it.
           Mantissa = [lists:flatten(io_lib:format("~2..0B", [MantissaDigit - 1]))
                       || << MantissaDigit >> <= MantissaBytes],
           % To calculate the decimal exponent, add 65 to the base-100 exponent
%io:format(user, "MantissaBytes ~p~nMantissa ~p~n", [MantissaBytes, Mantissa]),
           ?TO_STR(Mantissa, E - 65);
       true -> "0"
    end;
from_num(<< Len:8/integer, 0:1/integer-unit:1, E:7/integer-unit:1, Rest/bytes >>) ->
    MantissaLen = Len - 1,
    << MantissaB:MantissaLen/bytes, _/bytes >> = Rest,
    % Negative numbers have a byte containing 102 appended to the data bytes.
    % However, negative numbers that have 20 mantissa bytes do not have the
    % trailing 102 byte. Because the mantissa digits are stored in base 100,
    % each byte can represent 2 decimal digits
    MantissaBytes =
        if 20 =:= MantissaLen ->
               MantissaB;
           true ->
               NewMantissaLength = MantissaLen - 1,
               << NewMantissaB:NewMantissaLength/bytes, 102 >> = MantissaB,
               NewMantissaB
        end,
    % Each mantissa byte is a base-100 digit, in the range 1..100. For negative
    % numbers, instead of adding 1, the digit is subtracted from 101.
    Mantissa = [lists:flatten(io_lib:format("~2..0B", [101 - MantissaDigit]))
                || << MantissaDigit >> <= MantissaBytes],
    % If the number is negative, you do the same, but subsequently the bits are
    % inverted.
%io:format(user, "MantissaBytes ~p~nMantissa ~p~n", [MantissaBytes, Mantissa]),
    << Exp/integer >> = << (bnot E)/integer >>,
    [$- | ?TO_STR(Mantissa, Exp - 128 - 65)].

strip(List) ->
    case re:run(List, "\\.") of
        % No decimal point
        % no need to check for trailing zeros
        nomatch ->
            % removing only leading zero
            case List of
                [$0|Rest] -> Rest;
                List -> List
            end;
        % Has decimal point
        _ ->
            % removing leading zero
            % only if not < 1
            List1 = case List of
                [$0,$.|_] -> List;
                [$0|Rest] -> Rest;
                List -> List
            end,
            % removing tariling zeros
            case lists:reverse(List1) of
                [$0|Rest1] -> lists:reverse(Rest1);
                _ -> List1
            end
    end.

ins_dp([], 0) -> [];
ins_dp([], DP) when DP > 0 -> ["00"|ins_dp([], DP-1)];
ins_dp([H|[]], DP) when DP =:= 0 -> [H];
ins_dp([H|T], DP) when DP =:= 0 -> [H, "." | T];
ins_dp([H|[]], DP) when DP > 0 -> [H | ins_dp(["00"], DP-1)];
ins_dp([H|T], DP) when DP > 0 -> [H | ins_dp(T, DP-1)];
ins_dp(M, DP) when DP =:= -1 -> ins_dp(["0"|M], DP+1);
ins_dp(M, DP) when DP < 0 -> ins_dp(["00"|M], DP+1).

%% - % strip all spaces from both ends
%% - to_num([$ |_] = Num) -> to_num(string:strip(Num));
%% - to_num("") -> exit(invalid_number);
%% - to_num("-") -> exit(invalid_number);
%% - to_num("0") -> <<1,128>>;
%% - to_num(Num) ->
%% -     case Num of
%% -         % -ve number
%% -         [$-|Rest] ->
%% -             {E, Digits} = expo_digits(Rest),
%% -             << length(Num) + 1
%% -                , 0:1/integer-unit:1
%% -                , bnot (E+128+65):7/integer-unit:1
%% -                , list_to_binary(Digits)/bytes
%% -             >>;
%% -         % +ve number
%% -         Num ->
%% -             {E, Digits} = expo_digits(Num),
%% -             << length(Num) + 1
%% -                , 1:1/integer-unit:1
%% -                , (E+128+65):7/integer-unit:1
%% -                , list_to_binary(Digits)/bytes
%% -             >>
%% -     end.
%% - 
%% - tidy(Num) ->
%% -     % leading and trailing spaces and and then leading zeros removed
%% -     % except for < 1 numbers where 0. is added
%% -     {Dot, Num2} =
%% -         case string:strip(string:strip(Num), left, $0) of
%% -             [$.|T] -> {true,[$0,$.|T]};
%% -             Num1 -> {false,Num1}
%% -         end,
%% -     .
%% - % trailing zero removed only if decimal
%% - tidy(Dot,[],Tidy) ->
%% -     if Dot -> string:strip(Tidy, right, $0);
%% -        true -> Tidy
%% -     end;
%% - % 0.
%% - tidy(_,[$0,$.|Num],Tidy) -> tidy(true,Num,[$0,$.|Tidy]);
%% - % has dot
%% - tidy(false,[$0|Num],Tidy) -> tidy(Dot,Num,[D|Tidy]);
%% - tidy(false,[D|Num],Tidy) -> tidy(Num,[D|Tidy]);
%% - 
%% - 
%% - expo_digits(Num) ->
%% -     Num1 = 
%% -     expo_digits({0,[]}, Num).
%% - expo_digits({E,Num},[]) -> {E, Num};
%% - % number with leading decimal and zeros (in pair)
%% - expo_digits({E,Acc},[$0,$.,$0,$0|Rest]) ->
%% -     expo_digits({E+1,Acc},[$0,$.|Rest]);
%% - % number with leading decimal
%% - expo_digits({E,Acc},[$0,$.|Rest]) ->
%% -     expo_digits({E
%% -                  , Acc ++
%% -                  if (length(Rest) rem 2) /= 0 ->
%% -                         Rest++[$0];
%% -                     true -> Rest
%% -                  end
%% -                 },[]);
%% - 
%% - expo_digits({E,Acc},[$.|Rest]) -> {E,lists:reverse(Acc)++Rest};
%% - expo_digits({E,Acc},[D|Num]) -> expo_digits({E+1,[D|Acc],Num).
%% - 
%% - % Digits
%% - digits([]) -> [];
%% - digits(Num)
%% -   when (length(Num) rem 2) /= 0 -> digits([$0|Num]);
%% - digits([N1,N2|Num]) -> [[N1,N2]|digits(Num)].

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
    <<DateTimeNsBin/bytes, TzH:1/integer-unit:8, TzM:1/integer-unit:8>>;
to_dts({{Year, Month, Day}, {Hour, Minute, Second}, Ns}) ->
    DateTimeBin = to_dts({{Year, Month, Day}, {Hour, Minute, Second}}),
    % 11 bytes
    <<DateTimeBin/bytes, Ns:4/little-unsigned-integer-unit:8>>;
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

% Tests generated using:
% select dump(0.999999999999999999999999999999999999999) from dual;
num_conv_test_() ->
    {inparallel
     , [{T, fun() ->
                    ?assertEqual(T, from_num(S)),
                    %?assertEqual(S, to_num(T)),
                    ok
            end}
        || {T,S} <-
           [
            % Simple numbers
             {"0",             <<1,128>>}
           , {"1",             <<2,193,2>>}
           , {"0.1",           <<2,192,11>>}
           , {"10",            <<2,193,11>>}
           , {"1000",          <<2,194,11>>}
           , {"789.564",       <<5,194,8,90,57,41>>}
           , {"-1",            <<3,62,100,102>>}
           , {"-0.1",          <<3,63,91,102>>}
           , {"-0.01",         <<3,63,100,102>>}
           , {"-5678",         <<4,61,45,23,102>>}
           , {"-789.54",       <<5,61,94,12,47,102>>}
           , {"0.001234",      <<3,191,13,35>>}
           , {"0.0001234",     <<4,191,2,24,41>>}
           , {"0.0000123",     <<3,190,13,31>>}
           , {"-0.001234",     <<4,64,89,67,102>>}
           , {"-0.0001234",    <<5,64,100,78,61,102>>}
           , {"-0.0000123",    <<4,65,89,71,102>>}

           % Largest(+/-) whole and decimal numbers
           , {"9999999999999999999999999999999999999999"
              , <<21,212,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100>>}
           , {"99999999999999999999999999999999999999.9"
              , <<21,211,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,91>>}
           , {"9.99999999999999999999999999999999999999"
              , <<21,193,10,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100>>}
           , {"0.999999999999999999999999999999999999999"
              , <<21,192,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,91>>}
           , {"-9999999999999999999999999999999999999999"
              , <<21,43,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2>>}
           , {"-99999999999999999999999999999999999999.9"
              , <<21,44,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,11>>}
           , {"-9.99999999999999999999999999999999999999"
              , <<21,62,92,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2>>}
           , {"-0.999999999999999999999999999999999999999"
              , <<21,63,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,11>>}
           , {"1234567890123456789012345678901234567890"
              , <<21, 212,13,35,57,79,91,13,35,57,79,91,13,35,57,79,91,13,35,57,79,91>>}

           % Bigger than largest whole numbers
           , {"100000000000000000000000000000000000000000",     <<2,213,11>>}
           , {"1000000000000000000000000000000000000000000",    <<2,214,2>>}
           , {"10000000000000000000000000000000000000000000",   <<2,214,11>>}
           , {"-10000000000000000000000000000000000000000",     <<3,42,100,102>>}
           , {"-100000000000000000000000000000000000000000",    <<3,42,91,102>>}
           ]
       ]
    }.
-endif.
