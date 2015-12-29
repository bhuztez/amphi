-module(client).

-compile(export_all).


get(Key) ->
    Pid = controller:pick(),
    node:get(Pid, Key).


put(Key, Value) ->
    Pid = controller:pick(),
    node:put(Pid, Key, Value).
    
