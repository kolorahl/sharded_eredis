%%%-------------------------------------------------------------------
%%% @author Jeremy Ong <jeremy@playmesh.com>
%%% @copyright (C) 2012, PlayMesh, Inc.
%%%-------------------------------------------------------------------

-module(sharded_eredis).

%% Do not auto-import certain BIFs that could cause naming conflicts
-compile({no_auto_import,[get/1]}).

%% Include
-include_lib("eunit/include/eunit.hrl").

%% Default timeout for calls to the client gen_server
%% Specified in http://www.erlang.org/doc/man/gen_server.html#call-3
-define(TIMEOUT, 5000).

-export([start/0, stop/0]).

%% API
-export([q/1, q/2, q2/2, q2/3, transaction/2]).

-export([get/1, set/1, del/1]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

-type q_result() :: {ok, binary() | [binary()]} | {error, Reason::binary()}.

%% @doc Query the redis server using eredis. Automatically hashes
%% the key and selects the correct shard
-spec q(Command::iolist()) -> q_result().
q(Command) ->
    q(Command, ?TIMEOUT).

-spec q(Command::iolist(), Timeout::integer()) -> q_result().
q(Command = [_, Key|_], Timeout) ->
    Node = sharded_eredis_chash:lookup(Key),
    poolboy:transaction(Node, fun(Worker) ->
                                      eredis:q(Worker, Command, Timeout)
                              end).

%% @doc Supply a key and a function to perform a transaction.
%% It is NOT CHECKED but assumed that the function only performs operations
%% on that key or keys sharing the same node as that key.
transaction(Key, Fun) when is_function(Fun) ->
    Node = sharded_eredis_chash:lookup(Key),
    F = fun(C) ->
                try
                  {ok, <<"OK">>} = eredis:q(C, ["MULTI"]),
                  Fun(),
                  eredis:q(C, ["EXEC"])
                catch C:Reason ->
                       {ok, <<"OK">>} = eredis:q(C, ["DISCARD"]),
                       io:format("Error in redis transaction. ~p:~p", 
                                 [C, Reason]),
                       {error, Reason}
               end
    end,
    poolboy:transaction(Node, F).    

%% @doc q2 is similar to q but allows the user to specify the node
-spec q2(Node::term(), Command::iolist()) -> q_result().
q2(Node, Command) ->
    q2(Node, Command, ?TIMEOUT).

-spec q2(Node::term(), Command::iolist(), Timeout::integer()) -> q_result().
q2(Node, Command, Timeout) ->
    poolboy:transaction(Node, fun(Worker) ->
                                      eredis:q(Worker, Command, Timeout)
                              end).

%% Perform a query and pass successful return values to the given converter
%% function. Errors are returned without any conversion.
perform_q(Query, Converter) ->
    case q(Query) of
        {ok, Value} ->
            Converter(Value);
        Error ->
            Error
    end.

%% Alias to `perform_q/2`, where the converter function does no actual
%% conversion and simply returns the result as-is.
perform_q(Query) ->
    perform_q(Query, fun(Value) -> Value end).

%% Return a binary string representing the value of the given key, or the atom
%% `undefined` if there is no such key.
%%
%% If retrieving a list of keys, return an array with tuple elements in the form
%% of `{Key, Value}`. The return structure is essentially a proplist where the
%% "key" of each tuple is the Redis key, and the "value" is the Redis value.
%%
%% Note: Using this function with a list of keys is less efficient than using
%% the query (`q/1` and `q2/1`) functions **if** you know that all of your keys
%% are on the same node. If you know they exist on separate nodes, or are unsure
%% if they do, then this function is safer but slower.
get(Keys) when is_list(Keys) ->
    get_keys(Keys, []);
get(Key) ->
    perform_q(["GET", Key]).

%% Return the atom `ok` if the operation was successful, otherwise return an
%% error tuple.
set({Key, Value}) ->
    perform_q(["SET", Key, Value], fun(<<"OK">>) -> ok end).

%% Return an integer representing the number of keys/values that were deleted.
%%
%% Note: Using this function with a list of keys is less efficient than using
%% the query (`q/1` and `q2/1`) functions **if** you know that all of your keys
%% are on the same node. If you know they exist on separate nodes, or are unsure
%% if they do, then this function is safer but slower.
del(Keys) when is_list(Keys) ->
    del_keys(Keys, 0);
del(Key) ->
    perform_q(["DEL", Key], fun(Count) -> binary_to_integer(Count) end).

%% Used to get a set of keys in a single function call.
get_keys([], Acc) ->
    Acc;
get_keys([Key|Keys], Acc) ->
    Val = get(Key),
    get_keys(Keys, [{Key, Val}|Acc]).

%% Used to delete a set of keys in a single function call.
del_keys([], Count) ->
    Count;
del_keys([Key|Keys], Count) ->
    case del(Key) of
        X when is_integer(X) ->
            del_keys(Keys, Count + X);
        _ ->
            del_keys(Keys, Count)
    end.
