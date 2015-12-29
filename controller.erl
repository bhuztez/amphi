-module(controller).

-compile(export_all).


start() ->
    Ref = make_ref(),
    Pid = spawn(?MODULE, init, [self(), Ref]),
    register(?MODULE, Pid),
    receive
        {Pid, Ref, Nodes} ->
            lists:usort(Nodes)
    end.

stop() ->
    ?MODULE ! stop,
    unregister(?MODULE).

new() ->
    call(new_node).

kill(NodeID) ->
    call({kill_node, NodeID}).

nodes() ->
    lists:usort(call(get_nodes)).

pick() ->
    call(pick_node).

call(Request) ->
    Pid = whereis(?MODULE),
    true = Pid =/= undefined,
    Ref = make_ref(),
    Pid ! {call, self(), Ref, Request},
    receive
        {reply, Pid, Ref, Response} ->
            Response
    end.

init(Self, Ref) ->
    process_flag(trap_exit, true),
    Nodes = [node:start_bootstrap() || _ <- lists:seq(1,5)],
    Self ! {self(), Ref, [NodeID || {NodeID, _} <- Nodes]},
    [Pid ! {self(), bootstrap, Nodes} || {_, Pid} <- Nodes],
    ?MODULE:loop(Nodes).

loop(Nodes) ->
    receive
        {'EXIT', From, Reason} ->
            case lists:keyfind(From, 2, Nodes) of
                false ->
                    ?MODULE:loop(Nodes);
                {NodeID, From} ->
                    io:format("~p quit:~n ~p~n", [NodeID, Reason]),
                    ?MODULE:loop(proplists:delete(NodeID, Nodes))
            end;
        {call, From, Ref, pick_node} ->
            {_, Pid} = pick(Nodes),
            From ! {reply, self(), Ref, Pid},
            ?MODULE:loop(Nodes);
        {call, From, Ref, new_node} ->
            {_, Pid} = pick(Nodes),
            Node = {NodeID,_} = node:start_join(Pid),
            From ! {reply, self(), Ref, NodeID},
            ?MODULE:loop([Node|Nodes]);
        {call, From, Ref, {kill_node, NodeID}} ->
            case proplists:lookup(NodeID, Nodes) of
                none ->
                    From ! {reply, self(), Ref, not_found},
                    ?MODULE:loop(Nodes);
                {NodeID, Pid} ->
                    exit(Pid, kill),
                    From ! {reply, self(), Ref, ok},
                    ?MODULE:loop(Nodes)
            end;
        {call, From, Ref, get_nodes} ->
            From ! {reply, self(), Ref, [ID || {ID, _} <- Nodes]},
            ?MODULE:loop(Nodes);
        stop ->
            exit(shutdown)
    end.

pick([], X, _) ->
    X;
pick([H|T], X, N) ->
    case random:uniform(N) of
        1 ->
            pick(T, H, N+1);
        _ ->
            pick(T, X, N+1)
    end.

pick([H|T]) ->
    pick(T, H, 2).
