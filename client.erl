-module(client).

-compile(export_all).


get(Key) ->
    request({get, Key}).

put(Key, Value) ->
    request({put, Key, Value}).

request(Request) ->
    case node:request(controller:pick(), Request) of
        {ok, Response} ->
            Response;
        {error, timeout} ->
            request(Request)
    end.
