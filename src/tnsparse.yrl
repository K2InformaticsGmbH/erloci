Header "%% Copyright (C) K2 Informatics GmbH"
"%% @private"
"%% @Author Bikram Chatterjee"
"%% @Email bikram.chatterjee@k2informatics.ch".

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

Nonterminals
 configuration_file
 parameter
 keyword
 value
 value_list
 quoted_string
 params_list
.

Terminals
 LEFT_PAREN
 RIGHT_PAREN
 EQUALS
 COMMA
 SINGLE_QUOTE
 DOUBLE_QUOTE
 WORD
.

Rootsymbol configuration_file.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

configuration_file -> '$empty'                                                                  : [].
configuration_file -> parameter configuration_file                                              : ['$1'] ++ '$2'.

parameter -> keyword EQUALS value                                                               : [].
parameter -> keyword EQUALS LEFT_PAREN value_list RIGHT_PAREN                                   : [].
parameter -> keyword EQUALS LEFT_PAREN parameter RIGHT_PAREN                                    : [].

params_list -> '$empty'                                                                         : [].
params_list -> LEFT_PAREN parameter RIGHT_PAREN params_list                                     : [].

keyword -> WORD                                                                                 : [].

value -> WORD                                                                                   : [].
value -> quoted_string                                                                          : [].

value_list -> value COMMA value                                                                 : [].
value_list -> value_list COMMA value                                                            : [].

quoted_string -> SINGLE_QUOTE SINGLE_QUOTE                                                      : [].
quoted_string -> DOUBLE_QUOTE DOUBLE_QUOTE                                                      : [].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

Erlang code.


