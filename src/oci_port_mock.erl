%% Copyright 2012 K2Informatics GmbH, Root Laengenbold, Switzerland
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(oci_port_mock).
-compile([export_all]).

-include("oci.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(SessionId, 112233).
-define(Statement, 223344).

open_port({spawn_executable, _Executable}, _Options) ->
    Port = fun
        (F, undefined) ->
            QH = qlc:q([R||R<-rows()]),
            F(F, qlc:cursor(QH));
        (F, C) ->
            receive
                {port_cmd, Pid, {?R_DEBUG_MSG, 0}} ->
                    reply(Pid, F, C, {?R_DEBUG_MSG, ok, log_disabled});
                {port_cmd, Pid, {?R_DEBUG_MSG, 1}} ->
                    reply(Pid, F, C, {?R_DEBUG_MSG, ok, log_enabled});
                {port_cmd, Pid, {?CREATE_SESSION_POOL,_,_,_,_}} ->
                    reply(Pid, F, C, {?CREATE_SESSION_POOL, ok, "mock_pool"});
                {port_cmd, Pid, {?RELEASE_SESSION_POOL}} ->
                    reply(Pid, F, C, {?RELEASE_SESSION_POOL, ok});
                {port_cmd, Pid, {?GET_SESSION}} ->
                    reply(Pid, F, C, {?GET_SESSION, ok, ?SessionId});
                {port_cmd, Pid, {?RELEASE_SESSION, ?SessionId}} ->
                    reply(Pid, F, C, {?RELEASE_SESSION, ?SessionId, {ok}});
                {port_cmd, Pid, {?RELEASE_SESSION, _}} ->
                    reply(Pid, F, C, {?RELEASE_SESSION, ?SessionId, {error, mock_error_wrong_session}});
                {port_cmd, Pid, {?EXEC_SQL, ?SessionId, _, []}} ->
                    reply(Pid, F, C, {?EXEC_SQL, ?SessionId, {columns, ?Statement, columns()}});
                {port_cmd, Pid, {?EXEC_SQL, ?SessionId, _, _}} ->
                    reply(Pid, F, C, {?EXEC_SQL, ?SessionId, {executed, "Batman"}});
                {port_cmd, Pid, {?FETCH_ROWS, ?SessionId, ?Statement}} ->
                    Rows = qlc:next_answers(C, 10),
                    {QStatus, NewC} =
                    case length(Rows) of
                       L when L < 10 ->
                            {done, undefined}; %% cursor will be reinitialized
                       L when L == 10 ->
                            {more, C}
                    end,
                    reply(Pid, F, NewC, {?FETCH_ROWS, ?SessionId, {{rows, Rows}, QStatus}});
                {port_stop, Pid} ->
                    Pid ! ok;
                M ->
                    io:format("unrecognized port command ~p~n", [M]),
                    F(F, C)
            end
    end,
    spawn(fun() -> Port(Port, undefined) end).

port_close(Port) ->
    Port ! {port_stop, self()}.

port_info(_Port) ->
    [{name, "erlocimock"}, {id, 1234}, {mode, mock}].

port_command(Port, Msg) ->
    Port ! {port_cmd, self(), binary_to_term(Msg)},
    true.

reply(Pid, F, C, Msg) ->
    Pid ! {self(), {data, term_to_binary(Msg)}},
    F(F, C).

columns() ->
    [
        {"PKEY", integer, 100},
        {"PUBLISHER", string, 100},
        {"RANK", integer, 2},
        {"HERO", string, 100},
        {"REAL", string, 100},
        {"VOTES", integer, 4},
        {"VOTES_FIRST_RANK", integer, 4}
    ].

%% Credits for the data belongs to TOP 100 DC AND MARVEL CHARACTERS – MASTER LIST by comicbookresources.com
rows() ->
    [
        [1  , "DC", 1, "Batman", "Bruce Wayne", 2527, 118],
        [2  , "DC", 2, "Superman", "", 1326, 49],
        [3  , "DC", 3, "Flash", "Wally West", 1275, 30],
        [4  , "DC", 4, "Green Lantern", "Hal Jordan", 1091, 25],
        [5  , "DC", 5, "The Joker", "", 797, 9],
        [6  , "DC", 6, "Robin/Nightwing", "Dick Grayson", 665, 11],
        [7  , "DC", 7, "Green Arrow", "Oliver Queen", 552, 12],
        [8  , "DC", 8, "Wonder Woman", "", 542, 13],
        [9  , "DC", 9, "Robin", "Tim Drake", 502, 3],
        [10 , "DC", 10, "Batgirl", "Barbara Gordon", 470, 3],
        [11 , "DC", 11, "Martian Manhunter", "J’onn J’onzz", 438, 5],
        [12 , "DC", 12, "Booster Gold", "", 428, 7],
        [13 , "DC", 13, "The Question", "Vic Sage", 395, 4],
        [14 , "DC", 14, "Starman", "Jack Knight", 376, 12],
        [15 , "DC", 15, "Blue Beetle", "Ted Kord", 370, 6],
        [16 , "DC", 16, "Green Lantern", "Kyle Rayner", 364, 6],
        [17 , "DC", 17, "Rorschach", "", 363, 5],
        [18 , "DC", 18, "Black Canary", "", 352, 4],
        [19 , "DC", 19, "John Constantine", "", 344, 8],
        [20 , "DC", 19, "Lex Luthor", "",  344, 1],
        [21 , "DC", 21, "Animal Man", "", 325, 7],
        [22 , "DC", 21, "Hawkman", "Carter Hall", 325, 6],
        [23 , "DC", 23, "Dream/Morpheus", "", 322, 2],
        [24 , "DC", 24, "Zatanna", "", 307, 3],
        [25 , "DC", 25, "Green Lantern", "Guy Gardner", 303, 4],
        [26 , "DC", 26, "Darkseid", "", 293, 3],
        [27 , "DC", 27, "Captain Marvel", "Billy Batson", 289, 5],
        [28 , "DC", 27, "Power Girl", "", 289, 5],
        [29 , "DC", 29, "Flash", "Barry Allen", 281, 4],
        [30 , "DC", 30, "Aquaman", "Orin", 269, 6],
        [31 , "DC", 31, "Catwoman", "", 251, 1],
        [32 , "DC", 32, "Jesse Custer", "", 230, 9],
        [33 , "DC", 33, "Death", "", 229, 2],
        [34 , "DC", 34, "Spider Jerusalem", "", 225, 9],
        [35 , "DC", 34, "Swamp Thing", "Alec Holland", 225, 3],
        [36 , "DC", 36, "Firestorm", "", 201, 4],
        [37 , "DC", 37, "Green Lantern", "Alan Scott", 196, 3],
        [38 , "DC", 38, "Black Adam", "", 195, 0],
        [39 , "DC", 39, "Yorick Brown", "", 186, 2],
        [40 , "DC", 40, "Deathstroke the Terminator", "", 171, 0],
        [41 , "DC", 41, "Flash", "Jay Garrick", 170, 1],
        [42 , "DC", 42, "Impulse/Kid Flash", "Bart Allen",  169, 5],
        [43 , "DC", 42, "Bizarro", "", 169, 0],
        [44 , "DC", 44, "Plastic Man", "", 163, 3],
        [45 , "DC", 45, "Jonah Hex", "", 162, 5],
        [46 , "DC", 46, "Ambush Bug", "", 157, 6],
        [47 , "DC", 47, "Elongated Man", "", 153, 1 ],
        [48 , "DC", 48, "The Spectre", "Jim Corrigan", 149, 1 ],
        [49 , "DC", 49, "Mr. Miracle", "Scott Free", 147, 0],
        [50 , "DC", 50, "Huntress", "Helena Bertenelli", 125, 2],
        [51 , "Marvel", 1, "Spider-Man", "Peter Parker", 2164, 103],
        [52 , "Marvel", 2, "Captain America", "Steve Rogers", 1616, 43],
        [53 , "Marvel", 3, "Daredevil", "Matt Murdock", 1006, 22],
        [54 , "Marvel", 4, "Dr. Doom", "", 872, 15],
        [55 , "Marvel", 5, "The Thing", "Ben Grimm", 850, 12],
        [56 , "Marvel", 6, "Hulk", "Joe Fixit", 730, 15 ],
        [57 , "Marvel", 7, "Wolverine", "", 728, 10],
        [58 , "Marvel", 8, "Hawkeye", "", 676, 17],
        [59 , "Marvel", 9, "Thor", "", 605, 12],
        [60 , "Marvel", 10, "Cyclops", "", 604, 13],
        [61 , "Marvel", 11, "Iron Man", "Tony Stark", 603, 11],
        [62 , "Marvel", 12, "Nightcrawler", "", 497, 12],
        [63 , "Marvel", 13, "Magneto", "", 465, 7],
        [64 , "Marvel", 14, "Dr. Strange", "", 464, 12],
        [65 , "Marvel", 15, "Silver Surfer", "", 461, 8],
        [66 , "Marvel", 16, "Beast", "", 456, 8],
        [67 , "Marvel", 17, "Deadpool", "", 443, 13],
        [68 , "Marvel", 18, "Shadowcat", "Kitty Pryde", 427, 9],
        [69 , "Marvel", 19, "The Multiple Man", "Jamie Madrox", 399, 11],
        [70 , "Marvel", 20, "Phoenix", "Jean Grey", 374, 13],
        [71 , "Marvel", 21, "The Punisher", "", 360, 8],
        [72 , "Marvel", 22, "The White Queen", "Emma Frost", 332, 1],
        [73 , "Marvel", 23, "Iron Fist", "", 325, 0],
        [74 , "Marvel", 24, "Thanos", "", 287, 5],
        [75 , "Marvel", 25, "She-Hulk", "", 283, 9],
        [76 , "Marvel", 26, "Ultimate Spider-Man", "", 261, 12],
        [77 , "Marvel", 27, "Sub-Mariner", "Namor McKenzie", 222, 2],
        [78 , "Marvel", 27, "Nova", "", 222, 6],
        [79 , "Marvel", 29, "Nick Fury", "", 221, 2],
        [80 , "Marvel", 30, "Storm", "", 209, 3],
        [81 , "Marvel", 31, "Colossus", "", 208, 2],
        [82 , "Marvel", 32, "Galactus", "", 191, 4],
        [83 , "Marvel", 33, "Power Man", "Luke Cage", 190, 1],
        [84 , "Marvel", 34, "Adam Warlock", "", 186, 3],
        [85 , "Marvel", 35, "Ms. Marvel", "", 185, 0],
        [86 , "Marvel", 36, "Rogue", "", 183, 2],
        [87 , "Marvel", 37, "Invisible Woman", "Sue Storm/Richards", 170, 1],
        [88 , "Marvel", 38, "Princess Powerful/Bruiser", "Molly Hayes", 167, 1],
        [89 , "Marvel", 39, "Human Torch", "Johnny Storm", 163, 0],
        [90 , "Marvel", 40, "Ultimate Captain America", "", 161, 1],
        [91 , "Marvel", 41, "Gambit", "",  147, 5],
        [92 , "Marvel", 42, "The Vision", "", 146, 0],
        [93 , "Marvel", 43, "Mary Jane Watson", "", 144, 0],
        [94 , "Marvel", 44, "Black Panther", "", 137, 4],
        [95 , "Marvel", 45, "Jessica Jones", "", 134, 3],
        [96 , "Marvel", 46, "Iceman", "", 132, 5],
        [97 , "Marvel", 47, "Howard the Duck", "", 131, 1],
        [98 , "Marvel", 48, "J. Jonah Jameson", "", 130, 2],
        [99 , "Marvel", 49, "Mr.  Fantastic", "Reed Richards", 128, 0],
        [100, "Marvel", 49, "Bucky/Winter Soldier", "James Buchanan Barnes", 128, 0]
    ].
