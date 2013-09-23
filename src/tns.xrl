Definitions.

Rules.

% hint
(\()                                                : {token, {'LEFT_PAREN', TokenLine, TokenChars}}.
(\))                                                : {token, {'RIGHT_PAREN', TokenLine, TokenChars}}.
(\=)                                                : {token, {'EQUALS', TokenLine, TokenChars}}.
(\,)                                                : {token, {'COMMA', TokenLine, TokenChars}}.
(\')                                                : {token, {'SINGLE_QUOTE', TokenLine, TokenChars}}.
(\")                                                : {token, {'DOUBLE_QUOTE', TokenLine, TokenChars}}.
[A-Za-z0-9<>/.:;-_$+*&!%?@\\]                       : {token, {'WORD', TokenLine, TokenChars}}.


% skips
([\s\t\r\n]+)                                       : skip_token.    %% white space and new line

% comments
((\#).*[\n])                                        : skip_token.


Erlang code.


