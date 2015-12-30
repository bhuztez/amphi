-module(node).

-compile(export_all).


request(Pid, Request) ->
    request(Pid, Request, 200).

request(Pid, Request, Timeout) ->
    Ref = make_ref(),
    Pid ! {call, self(), Ref, Request},
    receive
        {reply, Pid, Ref, Response} ->
            {ok, Response}
    after Timeout ->
            {error, timeout}
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


phash(Key) ->
    erlang:phash2(Key, 16#100000000).


add_range(Range, []) ->
    [Range];
add_range(Range = {_, End}, Ranges=[{Start, _}|_])
  when End < Start ->
    [Range|Ranges];
add_range(Range = {Start, _}, [H={_, End}|T])
  when Start > End ->
    [H|add_range(Range, T)];
add_range({S1, E1}, [{S2, E2}|Ranges]) ->
    [{min(S1,S2),max(E1,E2)}|Ranges].

del_range(_, []) ->
    [];
del_range({_, End}, Ranges=[{Start, _}|_])
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
            {Stores, _} = lists:nth(length(Peers) - 2, Peers),
            State =
                #{ nodeid      => NodeID,
                   peers       => Peers,
                   prev        => lists:last(Peers),
                   replicas    => lists:sublist(Peers, 2),
                   stores      => Stores,
                   dropped     => [],
                   copying     => dict:new(),
                   data        => new_data()
                 },
            report(State),
            ?MODULE:loop(State)
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
            start_notify_node_joined(Peers, self(), NodeID),

            {PrevID, _} = Prev = lists:last(Peers),
            start_read_from_replicas(lists:sublist(Peers, 3), get_absolute_range(NodeID, {PrevID, 0})),

            State =
                #{ nodeid      => NodeID,
                   peers       => Peers,
                   prev        => Prev,
                   replicas    => lists:sublist(Peers, 2),
                   stores      => PrevID,
                   dropped     => [],
                   copying     => dict:new(),
                   data        => new_data()
                 },
            report(State),
            ?MODULE:loop(State)
    end.


loop(State) ->
    NextState =
        receive
            {call, From, Ref, Request} ->
                case ?MODULE:handle_call(Request, State) of
                    {reply, Reply, State1} ->
                        From ! {reply, self(), Ref, Reply},
                        State1;
                    {spawn, Handler, State1} ->
                        spawn_link(?MODULE, request_handler, [self(), From, Ref, Handler]),
                        State1
                end;
            Info ->
                ?MODULE:handle_info(Info, State)
    end,
    report(State, NextState),
    ?MODULE:loop(NextState).


request_handler(Self, Client, Ref, {M, F, A}) ->
    case apply(M, F, [Self|A]) of
        {ok, Reply} ->
            Client ! {reply, Self, Ref, Reply};
        _ ->
            ok
    end.

forward_request(Self, Peer, Request) ->
    call(Self, Peer, Request).


coordinator(Hash, [{NodeID, _}=Peer|_])
  when Hash < NodeID ->
    Peer;
coordinator(Hash, [_|Peers]) ->
    coordinator(Hash, Peers).

find_coordinator(Hash, #{nodeid:=NodeID, prev:={PrevID, _}, peers:=Peers}) ->
    case get_relative_nodeid(NodeID, Hash) of
        Hash1 when Hash1 >= PrevID ->
            self;
        Hash1 ->
            coordinator(Hash1, Peers)
    end.

handle_call({get, Key}=Request, State=#{data:=Data, replicas:=Replicas}) ->
    Hash = phash(Key),
    case find_coordinator(Hash, State) of
        self ->
            {spawn, {?MODULE, read_entries, [Key, find_entries(Hash, Key, Data), Replicas]}, State};
        Peer ->
            {spawn, {?MODULE, forward_request, [Peer, Request]}, State}
    end;
handle_call({put, Key, Value}=Request, State=#{data:=Data, replicas:=Replicas}) ->
    Hash = phash(Key),
    case find_coordinator(Hash, State) of
        self ->
            Entry = {next_clock(find_entries(Hash, Key, Data)), Value},
            {spawn, {?MODULE, write_entry, [Key, Entry, Replicas]}, State#{data:=add_entry(Hash, Key, Entry, Data)}};
        Peer ->
            {spawn, {?MODULE, forward_request, [Peer, Request]}, State}
    end;
handle_call({node_joined, PeerPid, PeerID}, State=#{nodeid:=NodeID, peers:=Peers, prev:={PrevID,_}, copying:=Copying, data:=Data}) ->
    Peer = {get_relative_nodeid(NodeID, PeerID), PeerPid},
    Peers1 = ordsets:add_element(Peer, Peers),

    Copying1 =
        case Peers1 of
            [{}, Peer|_] ->
                start_copy_to_replica(Peer, get_absolute_range(NodeID, {PrevID, 0}), Copying, Data);
            [Peer|_] ->
                start_copy_to_replica(Peer, get_absolute_range(NodeID, {PrevID, 0}), Copying, Data);
            _ ->
                Copying
        end,

    State1 = State#{peers:=Peers1, copying:=Copying1},

    State2 =
        case lists:last(Peers1) of
            Peer ->
                State1#{prev:=Peer};
            _ ->
                State1
        end,
    {reply, ok, State2};
handle_call({node_down, PeerPid, PeerID}, State=#{nodeid:=NodeID, peers:=Peers}) ->
    Peer = {get_relative_nodeid(NodeID, PeerID), PeerPid},
    {reply, ok, handle_node_down(Peer, State#{peers:=ordsets:del_element(Peer, Peers)})};
handle_call(get_peers, State=#{nodeid:=NodeID, peers:=Peers}) ->
    {reply,
     [{get_absolute_nodeid(NodeID, ID), Pid}
      || {ID, Pid} <- [{0, self()}|Peers]],
     State};
handle_call({put_range, Range}, State=#{nodeid:=NodeID, stores:=Stores, dropped:=Dropped}) ->
    {RangeStart, _} = Range1 = get_relative_range(NodeID, Range),
    Stores1 =
        case RangeStart < Stores of
            true ->
                RangeStart;
            false ->
                Stores
        end,
    {reply, ok, State#{stores:=Stores1, dropped:=del_range(Range1, Dropped)}};
handle_call({drop_range, Range}, State=#{nodeid:=NodeID, stores:=Stores, dropped:=Dropped, data:=Data}) ->
    case add_range(get_relative_range(NodeID, Range), Dropped) of
        [{Stores, Stores1}|Dropped1] ->
            Data1 = drop_entries_in_range(get_absolute_range(NodeID, {Stores, Stores1}), Data),
            {reply, ok, State#{stores:=Stores1, dropped:=Dropped1, data:=Data1}};
        Dropped1 ->
            {reply, ok, State#{dropped:=Dropped1}}
    end;
handle_call({read_range, Range}, State=#{data:=Data}) ->
    {reply, read_entries_in_range(Range, Data), State};
handle_call({read, Key}, State=#{data:=Data}) ->
    {reply, find_entries(phash(Key), Key, Data), State};
handle_call({write, Key, Entries}, State=#{nodeid:=NodeID, stores:=Stores, data:=Data}) ->
    Hash = phash(Key),
    case get_relative_nodeid(NodeID, Hash) >= Stores of
        false ->
            {reply, not_found, State};
        true ->
            Data1 =
                lists:foldl(
                  fun (Entry, Acc) ->
                          add_entry(Hash, Key, Entry, Acc)
                  end,
                  Data,
                  Entries),
            {reply, ok, State#{data:=Data1}}
    end.


handle_info({copy_to_replica_success, Peer, Range}, State=#{nodeid:=NodeID, prev:={PrevID,_}, copying:=Copying, replicas:=Replicas}) ->
    case dict:find(Peer, Copying) of
        error ->
            State;
        {ok, Ranges} ->
            case lists:delete(Range, Ranges) of
                [] ->
                    State1 = State#{copying:=dict:erase(Peer, Copying)},
                    case ordsets:add_element(Peer, Replicas) of
                        [Peer1, Peer2, Peer3] ->
                            start_remove_replica(Peer3, get_absolute_range(NodeID, {PrevID, 0})),
                            State1#{replicas:=[Peer1, Peer2]};
                        Replicas1 ->
                            State1#{replicas:=Replicas1}
                    end;
                Ranges1 ->
                    State#{copying:=dict:store(Peer, Ranges1, Copying)}
            end
    end;
handle_info({peer_failure, Peer={PeerID, PeerPid}}, State=#{nodeid:=NodeID, peers:=Peers}) ->
    Peers1 = ordsets:del_element(Peer, Peers),
    start_notify_node_down(Peers1, PeerPid, get_absolute_nodeid(NodeID, PeerID)),
    handle_node_down(Peer, State#{peers:=Peers1});
handle_info(Info, State) ->
    io:format("Unknown message: ~p~n", [Info]),
    State.


handle_node_down(Peer={PeerID, _}, State=#{prev:=Peer, nodeid:=NodeID, peers:=Peers=[_,NextReplica|_], replicas:=Replicas, copying:=Copying, data:=Data}) ->
    Replicas1 = lists:sublist(ordsets:del_element(Peer, Replicas), 1),
    {Prev1ID, _}= Prev1 = lists:last(Peers),
    Copying1 = start_copy_to_replica(NextReplica, get_absolute_range(NodeID, {Prev1ID, PeerID}), dict:erase(Peer, Copying), Data),
    State#{prev:=Prev1, replicas:=Replicas1, copying:=Copying1};
handle_node_down(Peer, State=#{nodeid:=NodeID, peers:=[_,NextReplica|_], prev:={PrevID, _}, replicas:=Replicas, copying:=Copying, data:=Data}) ->
    Copying1 = dict:erase(Peer, Copying),
    Copying2 =
        case ordsets:del_element(Peer, Replicas) of
            [_] = Replicas1 ->
                start_copy_to_replica(NextReplica, get_absolute_range(NodeID, {PrevID, 0}), Copying1, Data);
            Replicas1 ->
                Copying1
        end,
    State#{replicas:=Replicas1, copying:=Copying2}.


start_copy_to_replica(Peer, Range, Copying, Data) ->
    spawn_link(?MODULE, copy_to_replica, [self(), Peer, Range, read_entries_in_range(Range, Data)]),
    dict:append(Peer, Range, Copying).

copy_to_replica(Self, Peer, Range, Entries) ->
    case call(Self, Peer, {put_range, Range}) of
        {ok, ok} ->
            case lists:all(
                   fun ({Key, E}) ->
                           {ok, ok} =:= call(Self, Peer, {write, Key, E})
                   end,
                   Entries) of
                true ->
                    Self ! {copy_to_replica_success, Peer, Range};
                false ->
                    ok
            end;
        _ ->
            ok
    end.

start_remove_replica(Peer, Range) ->
    spawn_link(?MODULE, remove_replica, [self(), Peer, Range]).

remove_replica(Self, Peer, Range) ->
    call(Self, Peer, {drop_range, Range}).

start_read_from_replicas(Peers, Range) ->
    spawn_link(?MODULE, read_from_replicas, [self(), Peers, Range]).

read_from_replicas(Self, Peers, Range) ->
    Results = lists:append([E ||  {ok, E} <- multicall(Self, Peers, {read_range, Range})]),
    [call(Self, {0, Self}, {write, Key, Entries}) || {Key, Entries} <- Results],
    remove_replica(Self, lists:last(Peers), Range),
    ok.

call(Self, {_,Pid}=Peer, Request) ->
    Result = request(Pid, Request),
    case Result of
        {error, timeout} ->
            Self ! {peer_failure, Peer};
        _ ->
            ok
    end,
    Result.

multicall(Self, Peers, Request) ->
    Pids =
        [ spawn_link(?MODULE, do_call, [self(), Self, Peer, Request])
          || Peer <- Peers ],
    [receive
         {result, Pid, Result} ->
             Result
     end
     || Pid <- Pids ].

do_call(Parent, Self, Peer, Request) ->
    Parent ! {result, self(), call(Self, Peer, Request)}.


start_notify_node_joined(Peers, PeerPid, PeerID) ->
    spawn_link(?MODULE, do_multicall, [self(), Peers, {node_joined, PeerPid, PeerID}]).

start_notify_node_down(Peers, PeerPid, PeerID) ->
    spawn_link(?MODULE, do_multicall, [self(), Peers, {node_down, PeerPid, PeerID}]).

do_multicall(Self, Peers, Request) ->
    multicall(Self, Peers, Request).


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
                          [Entry] ++ [E || {C, _} = E <- Entries, C =/= Clock, not is_earlier(C, Clock)]
                  end,
                  [Entry],
                  Dict),
            gb_trees:enter(Hash, Dict1, Data);
        none ->
            gb_trees:enter(Hash, dict:append(Key, Entry, dict:new()), Data)
    end.

read_entries(Self, Key, Have, Replicas) ->
    Entries = lists:usort(lists:append([E || {ok, E} <- multicall(Self, Replicas, {read, Key})])),

    Others =
        [ Entry
          || Entry = {Clock, _} <- Entries,
             not lists:any(
                   fun ({C,_})
                         when C =:= Clock
                              ->
                           true;
                       ({C,_}) ->
                           is_earlier(Clock, C)
                   end,
                   Have)],

    case Others of
        [] ->
            ok;
        _ ->
            call(Self, {0, Self}, {write, Key, Others})
    end,
    {ok, [V || {_,V} <- Have ++ Others]}.


write_entry(Self, Key, Entry, Replicas) ->
    multicall(Self, Replicas, {write, Key, [Entry]}),
    {ok, ok}.


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



report(#{nodeid:=NodeID, prev:={PrevID,_}, stores:=Stores, dropped:=Dropped}) ->
    Stores1 =
        lists:foldl(
          fun del_range/2,
          [{Stores, 0}],
          Dropped),
    io:format(
      "range change~nnode: ~p~ncoordinates: ~p~nstores: ~p~n~n",
      [ NodeID,
        get_absolute_range(NodeID, {PrevID, 0}),
        [get_absolute_range(NodeID, Range) || Range <- Stores1]
      ]).

report(#{prev:={PrevID,_}, stores:=Stores, dropped:=Dropped}, #{prev:={PrevID,_}, stores:=Stores, dropped:=Dropped}) ->
    ok;
report(_, State1) ->
    report(State1).
