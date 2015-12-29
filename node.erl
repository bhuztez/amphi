-module(node).

-compile(export_all).


get(Pid, Key) ->
    request(Pid, Key, get).

put(Pid, Key, Value) ->
    request(Pid, Key, {put, Value}).

request(Pid, Key, Request) ->
    Ref = make_ref(),
    Pid ! {request, Pid, self(), Ref, Key, Request},
    receive
        {response, Pid, Ref, Response} ->
            Response
    end.


get_relative_nodeid(Base, Abs) ->
    case (Abs - Base) rem 16#100000000 of
        Rel when Rel < 0 ->
            Rel + 16#100000000;
        Rel ->
            Rel
    end.

get_absolute_nodeid(Base, Rel) ->
    (Base + Rel) rem 16#100000000.

get_relative_range(Base, {S, E}) ->
    {get_relative_nodeid(Base, S), get_relative_nodeid(Base, E)}.

get_absolute_range(Base, {S, E}) ->
    {get_absolute_nodeid(Base, S), get_absolute_nodeid(Base, E)}.


add_range(Range, []) ->
    [Range];
add_range(Range = {_, End}, Ranges = [{Start, _}|_])
  when End < Start ->
    [Range|Ranges];
add_range(Range = {Start, _}, [H={_, End}|T])
  when Start > End ->
    [H|add_range(Range, T)];
add_range({S1, E1}, [{S2, E2}|Ranges]) ->
    [{min(S1,S2),max(E1,E2)}|Ranges].

del_range(_, []) ->
    [];
del_range({_, End}, Ranges = [{Start, _}|_])
  when End < Start ->
    Ranges;
del_range({Start, _}=Range, [H={_, End}|T])
  when Start > End ->
    [H|del_range(Range, T)];
del_range({S1, E1}, [{S2, E2}|Ranges])
  when S1 =< S2, E1 >= E2 ->
    Ranges;
del_range({S1, E1}, [{S2, E2}|Ranges])
  when S1 > S2, E1 < E2 ->
    [{S2, S1}, {E1, E2}|Ranges];
del_range({S1, E1}, [{S2, E2}|Ranges])
  when S1 =< S2, E1 < E2 ->
    [{E1, E2}|Ranges];
del_range({S1, E1}, [{S2, E2}|Ranges])
  when S1 > S2, E1 >= E2 ->
    [{S2, S1}|Ranges].



start_bootstrap() ->
    <<NodeID:32>> = crypto:rand_bytes(4),
    Pid = spawn_link(?MODULE, bootstrap, [self(), NodeID]),
    {NodeID, Pid}.


bootstrap(Controller, NodeID) ->
    receive
        {Controller, bootstrap, Nodes} ->
            Peers =
                ordsets:from_list(
                  [{get_relative_nodeid(NodeID, ID), Pid}
                   || {ID, Pid} <- Nodes,
                      ID =/= NodeID]),
            {PrevID, _} = Prev = lists:last(Peers),
            Replicas = lists:sublist(Peers, 2),
            Coordinates = PrevID,
            {StartId, _} = lists:nth(length(Peers) - 2, Peers),
            Stores = StartId,

            report(NodeID, Coordinates, Stores, []),

            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, [], dict:new(), new_data())
    end.


start_join(PeerPid) ->
    <<NodeID:32>> = crypto:rand_bytes(4),
    Pid = spawn_link(?MODULE, join, [PeerPid, NodeID]),
    {NodeID, Pid}.

join(PeerPid, NodeID) ->
    Ref = make_ref(),
    PeerPid ! {call, self(), Ref, get_peers},
    receive
        {reply, PeerPid, Ref, Nodes} ->
            Peers =
                ordsets:from_list(
                  [{get_relative_nodeid(NodeID, ID), Pid}
                   || {ID, Pid} <- Nodes,
                      ID =/= NodeID]),
            start_notify_node_joined(Peers, NodeID),
            {PrevID, _} = Prev = lists:last(Peers),
            Replicas = lists:sublist(Peers, 2),
            Coordinates = PrevID,
            start_read_from_replicas(lists:sublist(Peers, 3), {Coordinates, 0}, NodeID),
            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Coordinates, [], dict:new(), new_data())
    end.


loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying, Data) ->
    receive
        {request, OriginPeer, Client, Ref, Key, Request} = ClientRequest ->
            Hash = erlang:phash2(Key, 16#100000000),
            case get_relative_nodeid(NodeID, Hash) of
                Hash1 when Hash1 >= Coordinates ->
                    Have = find_entries(Hash, Key, Data),
                    case Request of
                        get ->
                            start_read_entries(OriginPeer, Client, Ref, Key, Have, Replicas),
                            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying, Data);
                        {put, Value} ->
                            Clock = next_clock(Have),
                            Data1 = add_entry(Hash, Key, {Clock, Value}, Data),
                            start_write_entry(OriginPeer, Client, Ref, Key, {Clock, Value}, Replicas),
                            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying, Data1)
                    end;
                Hash1 ->
                    PeerPid = coordinator(Hash1, Peers),
                    PeerPid ! ClientRequest,
                    ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying, Data)
            end;
        {copy_to_replica_success, Peer, Range} ->
            case dict:find(Peer, Copying) of
                {ok, Ranges} ->
                    case lists:delete(Range, Ranges) of
                        [] ->
                            Copying1 = dict:erase(Peer, Copying),
                            case ordsets:add_element(Peer, Replicas) of
                                [Peer1, Peer2, Peer3] ->
                                    start_remove_replica(Peer3, NodeID, {Coordinates, 0}),
                                    ?MODULE:loop(NodeID, Peers, Prev, [Peer1, Peer2], Coordinates, Stores, Dropped, Copying1, Data);
                                Replicas1 ->
                                    ?MODULE:loop(NodeID, Peers, Prev, Replicas1, Coordinates, Stores, Dropped, Copying1, Data)
                            end;
                        Ranges1 ->
                            Copying1 = dict:store(Peer, Ranges1, Copying),
                            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying1, Data)
                    end;
                error ->
                    ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying, Data)
            end;
        {peer_failure, Peer} ->
            [_,NextReplica|_] = Peers1 = ordsets:del_element(Peer, Peers),
            start_notify_node_down(Peer, Peers1, NodeID),
            Replicas1 = ordsets:del_element(Peer, Replicas),
            Copying1 = dict:erase(Peer, Copying),

            case Peer of
                Prev = {PrevID, _} ->
                    {Prev1ID, _}= Prev1 = lists:last(Peers1),
                    Coordinates1 = Prev1ID,
                    Replicas2 = lists:sublist(Replicas1, 1),
                    Copying2 = start_copy_to_replica(NextReplica, {Prev1ID, PrevID}, NodeID, Copying1),

                    report(NodeID, Coordinates1, Stores, Dropped),

                    ?MODULE:loop(NodeID, Peers1, Prev1, Replicas2, Coordinates1, Stores, Dropped, Copying2, Data);
                _ ->
                    case Replicas1 of
                        [_] ->
                            Copying2 = start_copy_to_replica(NextReplica, {Coordinates, 0}, NodeID, Copying),
                            ?MODULE:loop(NodeID, Peers1, Prev, Replicas1, Coordinates, Stores, Dropped, Copying2, Data);
                        _ ->
                            ?MODULE:loop(NodeID, Peers1, Prev, Replicas1, Coordinates, Stores, Dropped, Copying, Data)
                    end
            end;
        {call, From, Ref, {node_joined, PeerPid, PeerID}} ->
            From ! {reply, self(), Ref, ok},
            RelID = get_relative_nodeid(NodeID, PeerID),
            Peer = {RelID, PeerPid},
            Peers1 = ordsets:add_element(Peer, Peers),

            Copying1 =
                case Peers1 of
                    [{}, Peer|_] ->
                        start_copy_to_replica(Peer, {Coordinates, 0}, NodeID, Copying);
                    [Peer|_] ->
                        start_copy_to_replica(Peer, {Coordinates, 0}, NodeID, Copying);
                    _ ->
                        Copying
            end,

            case lists:last(Peers1) of
                Peer ->
                    report(NodeID, RelID, Stores, Dropped),
                    ?MODULE:loop(NodeID, Peers1, Peer, Replicas, RelID, Stores, Dropped, Copying1, Data);
                _ ->
                    ?MODULE:loop(NodeID, Peers1, Prev, Replicas, Coordinates, Stores, Dropped, Copying1, Data)
            end;

        {call, From, Ref, {node_down, PeerPid, PeerID}} ->
            From ! {reply, self(), Ref, ok},
            RelID = get_relative_nodeid(NodeID, PeerID),
            Peer = {RelID, PeerPid},
            [_,NextReplica|_] = Peers1 = ordsets:del_element(Peer, Peers),
            Replicas1 = ordsets:del_element(Peer, Replicas),
            Copying1 = dict:erase(Peer, Copying),

            case Peer of
                Prev = {PrevID, _} ->
                    {Prev1ID, _}= Prev1 = lists:last(Peers1),
                    Coordinates1 = Prev1ID,
                    Replicas2 = lists:sublist(Replicas1, 1),
                    Copying2 = start_copy_to_replica(NextReplica, {Prev1ID, PrevID}, NodeID, Copying1),

                    report(NodeID, Coordinates1, Stores, Dropped),

                    ?MODULE:loop(NodeID, Peers1, Prev1, Replicas2, Coordinates1, Stores, Dropped, Copying2, Data);
                _ ->
                    case Replicas1 of
                        [_] ->
                            Copying2 = start_copy_to_replica(NextReplica, {Coordinates, 0}, NodeID, Copying),
                            ?MODULE:loop(NodeID, Peers1, Prev, Replicas1, Coordinates, Stores, Dropped, Copying2, Data);
                        _ ->
                            ?MODULE:loop(NodeID, Peers1, Prev, Replicas1, Coordinates, Stores, Dropped, Copying, Data)
                    end
            end;
        {call, From, Ref, get_peers} ->
            From ! {reply, self(), Ref,
                    [{get_absolute_nodeid(NodeID, ID), Pid}
                     || {ID, Pid} <- [{0, self()}|Peers]]},
            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying, Data);
        {call, From, Ref, {put_range, Range}} ->
            {RangeStart, _} = Range1 = get_relative_range(NodeID, Range),
            Stores1 =
                case RangeStart < Stores of
                    true ->
                        RangeStart;
                    false ->
                        Stores
                end,
            From ! {reply, self(), Ref, ok},

            Dropped1 = del_range(Range1, Dropped),

            report(NodeID, Coordinates, Stores1, Dropped1),

            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores1, Dropped1, Copying, Data);
        {call, From, Ref, {drop_range, Range}} ->
            case add_range(get_relative_range(NodeID, Range), Dropped) of
                [{Stores, RangeEnd}|Dropped1] ->
                    Stores1 = RangeEnd,
                    From ! {reply, self(), Ref, ok},

                    report(NodeID, Coordinates, Stores1, Dropped1),
                    Data1 = drop_entries_in_range(get_absolute_range(NodeID, {Stores, Stores1}), Data),

                    ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores1, Dropped1, Copying, Data1);
                Dropped1 ->
                    From ! {reply, self(), Ref, ok},

                    report(NodeID, Coordinates, Stores, Dropped1),

                    ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped1, Copying, Data)
            end;
        {call, From, Ref, {read_range, Range}} ->
            Response = read_entries_in_range(Range, Data),
            From ! {reply, self(), Ref, Response},
            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying, Data);
        {call, From, Ref, {read, Key}} ->
            Hash = erlang:phash2(Key, 16#100000000),
            Entries = find_entries(Hash, Key, Data),
            From ! {reply, self(), Ref, Entries},
            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying, Data);
        {call, From, Ref, {write, Key, Entries}} ->
            Hash = erlang:phash2(Key, 16#100000000),
            Data1 =
                case get_relative_nodeid(NodeID, Hash) >= Stores of
                    true ->
                        From ! {reply, self(), Ref, ok},
                        lists:foldl(
                          fun (Entry, Acc) ->
                                  add_entry(Hash, Key, Entry, Acc)
                          end,
                          Data,
                          Entries);
                    false ->
                        From ! {reply, self(), Ref, error}
                end,
            ?MODULE:loop(NodeID, Peers, Prev, Replicas, Coordinates, Stores, Dropped, Copying, Data1)
    end.


coordinator(Hash, [{NodeID, Pid}|_])
  when Hash < NodeID ->
    Pid;
coordinator(Hash, [_|Peers]) ->
    coordinator(Hash, Peers).


start_copy_to_replica(Peer, Range, NodeID, Copying) ->
    Range1 = get_absolute_range(NodeID, Range),
    spawn_link(?MODULE, copy_to_replica, [self(), Peer, Range1]),
    dict:append(Peer, Range1, Copying).

copy_to_replica(Self, Peer, Range) ->
    case call(Self, Peer, {put_range, Range}, 200) of
        {ok, ok} ->
            Self ! {copy_to_replica_success, Peer, Range};
        _ ->
            ok
    end.

start_remove_replica(Peer, NodeID, Range) ->
    spawn_link(?MODULE, remove_replica, [self(), Peer, get_absolute_range(NodeID, Range)]).

remove_replica(Self, Peer, Range) ->
    call(Self, Peer, {drop_range, Range}, 200).


start_read_from_replicas(Peers, Range, NodeID) ->
    spawn_link(?MODULE, read_from_replicas, [self(), Peers, get_absolute_range(NodeID, Range)]).

read_from_replicas(Self, Peers, Range) ->
    Results = lists:append([E ||  {ok, E} <- multicall(Self, Peers, {read_range, Range}, 200)]),
    [call(Self, {0, Self}, {write, Key, Entries}, 200) || {Key, Entries} <- Results],
    remove_replica(Self, lists:last(Peers), Range),
    ok.

call(Self, {_,Pid}=Peer, Request, Timeout) ->
    Ref = make_ref(),
    Pid ! {call, self(), Ref, Request},
    receive
        {reply, Pid, Ref, Response} ->
            {ok, Response}
    after Timeout ->
            Self ! {peer_failure, Peer},
            {error, timeout}
    end.


multicall(Self, Peers, Request, Timeout) ->
    Pids =
        [ spawn_link(?MODULE, do_call, [self(), Self, Peer, Request, Timeout])
          || Peer <- Peers ],
    [receive
         {result, Pid, Result} ->
             Result
     end
     || Pid <- Pids ].

do_call(Parent, Self, Peer, Request, Timeout) ->
    Parent ! {result, self(), call(Self, Peer, Request, Timeout)}.


report(NodeID, Coordiates, Stores, Dropped) ->
    Stores1 =
        lists:foldl(
          fun del_range/2,
          [{Stores, 0}],
          Dropped),
    io:format(
      "range change~nnode: ~p~ncoordinates: ~p~nstores: ~p~n~n",
      [ NodeID,
        get_absolute_range(NodeID, {Coordiates, 0}),
        [get_absolute_range(NodeID, Range) || Range <- Stores1]
      ]).


start_notify_node_joined(Peers, NodeID) ->
    spawn_link(?MODULE, do_multicall, [self(), Peers, {node_joined, self(), NodeID}]).

start_notify_node_down({PeerID, PeerPid}, Peers, NodeID) ->
    spawn_link(?MODULE, do_multicall, [self(), Peers, {node_down, PeerPid, get_absolute_nodeid(NodeID, PeerID)}]).

do_multicall(Self, Peers, Request) ->
    multicall(Self, Peers, Request, 200).


next_clock([]) ->
    maps:put(self(), 1, maps:new());
next_clock(Entries) ->
    increase_clock(self(), latest([Clock || {Clock, _} <- Entries])).

increase_clock(Key, Clock) ->
    maps:put(Key, maps:get(Key, Clock, 0) + 1, Clock).

latest(Clocks) ->
    maps:from_list(
      dict:to_list(
        lists:foldl(
          fun (Clock, Acc) ->
                  dict:merge(
                    fun(_, T1, T2) -> max(T1, T2) end,
                    dict:from_list(maps:to_list(Clock)),
                    Acc)
          end,
          dict:new(),
          Clocks))).


is_earlier(C1, C2) ->
    is_earlier_aux(lists:usort(maps:to_list(C1)), lists:usort(maps:to_list(C2))).

is_earlier_aux([{K, Time1}|_], [{K, Time2}|_])
  when Time1 >= Time2 ->
    false;
is_earlier_aux([_|Rest1], [_|Rest2]) ->
    is_earlier_aux(Rest1, Rest2);
is_earlier_aux([{K1, _}|_], [{K2, _}|_])
  when K1 < K2 ->
    false;
is_earlier_aux([{K1, _}|_]=Times, [{K2, _}|Rest2])
  when K1 > K2 ->
    is_earlier_aux(Times, Rest2);
is_earlier_aux([], []) ->
    true.


new_data() ->
    gb_trees:empty().

find_entries(Hash, Key, Data) ->
    case gb_trees:lookup(Hash, Data) of
        {value, Dict} ->
            case dict:find(Key, Dict) of
                {ok, Entries} ->
                    Entries;
                error ->
                    []
            end;
        none ->
            []
    end.

add_entry(Hash, Key, Entry = {Clock, _}, Data) ->
    case gb_trees:lookup(Hash, Data) of
        {value, Dict} ->
            Dict1 =
                dict:update(
                  Key,
                  fun (Entries) ->
                          [Entry] ++ [E || {C, _} = E <- Entries, not is_earlier(C, Clock)]
                  end,
                  [Entry],
                  Dict),
            gb_trees:enter(Hash, Dict1, Data);
        none ->
            gb_trees:enter(Hash, dict:append(Key, Entry, dict:new()), Data)
    end.


start_read_entries(OriginPeer, Client, Ref, Key, Have, Replicas) ->
    spawn_link(?MODULE, read_entries, [self(), OriginPeer, Client, Ref, Key, Have, Replicas]).

read_entries(Self, OriginPeer, Client, Ref, Key, Have, Replicas) ->
    Entries = lists:usort(lists:append([E || {ok, E} <- multicall(Self, Replicas, {read, Key}, 200)])),

    Others = [Entry || Entry = {Clock, _} <- Entries, not lists:any(fun ({C,_}) when C =:= Clock -> true; ({C,_}) ->  is_earlier(Clock, C) end, Have)],

    case Others of
        [] ->
            ok;
        _ ->
            call(Self, {0, Self}, {write, Key, Others}, 200)
    end,
    Client ! {response, OriginPeer, Ref, lists:usort([V || {_,V} <- Have ++ Others])}.


start_write_entry(OriginPeer, Client, Ref, Key, Entry, Replicas) ->
    spawn_link(?MODULE, write_entry, [self(), OriginPeer, Client, Ref, Key, Entry, Replicas]).

write_entry(Self, OriginPeer, Client, Ref, Key, Entry, Replicas) ->
    multicall(Self, Replicas, {write, Key, [Entry]}, 200),
    Client ! {response, OriginPeer, Ref, ok}.


read_entries_in_range(Range, Data) ->
    lists:append(
      [dict:to_list(gb_trees:get(Hash, Data))
       || Hash <- gb_trees:keys(Data), in_range(Hash, Range)]).


drop_entries_in_range(Range, Data) ->
    lists:foldl(
      fun gb_trees:delete/2,
      Data,
      [Hash || Hash <- gb_trees:keys(Data), in_range(Hash, Range)]).


in_range(Hash, {S, E}) when S < E ->
    (Hash >= S) and (Hash < E);
in_range(Hash, {S, E}) ->
    (Hash >= S) or (Hash < E).
