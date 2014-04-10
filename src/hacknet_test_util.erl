-module(hacknet_test_util).

-export([pmap/2, pmap/3]).

%% @doc Invoke function `F' over each element of list `L' in parallel,
%%      returning the results in the same order as the input list.
-spec pmap(function(), [node()]) -> [any()].
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
      fun(X, N) ->
              spawn(fun() ->
                            Parent ! {pmap, N, F(X)}
                    end),
              N+1
      end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

-record(pmap_acc,{
                  mapper,
                  fn,
                  n_pending=0,
                  pending=sets:new(),
                  n_done=0,
                  done=[],
                  max_concurrent=1
                  }).

%% @doc Parallel map with a cap on the number of concurrent worker processes.
%% Note: Worker processes are linked to the parent, so a crash propagates.
-spec pmap(Fun::function(), List::list(), MaxP::integer()) -> list().
pmap(Fun, List, MaxP) when MaxP < 1 ->
    pmap(Fun, List, 1);
pmap(Fun, List, MaxP) when is_function(Fun), is_list(List), is_integer(MaxP) ->
    Mapper = self(),
    #pmap_acc{pending=Pending, done=Done} =
                 lists:foldl(fun pmap_worker/2,
                             #pmap_acc{mapper=Mapper,
                                       fn=Fun,
                                       max_concurrent=MaxP},
                             List),
    All = pmap_collect_rest(Pending, Done),
    % Restore input order
    Sorted = lists:keysort(1, All),
    [ R || {_, R} <- Sorted ].

%% @doc Fold function for {@link pmap/3} that spawns up to a max number of
%% workers to execute the mapping function over the input list.
pmap_worker(X, Acc = #pmap_acc{n_pending=NP,
                               pending=Pending,
                               n_done=ND,
                               max_concurrent=MaxP,
                               mapper=Mapper,
                               fn=Fn})
  when NP < MaxP ->
    Worker =
        spawn_link(fun() ->
                           R = Fn(X),
                           Mapper ! {pmap_result, self(), {NP+ND, R}}
                   end),
    Acc#pmap_acc{n_pending=NP+1, pending=sets:add_element(Worker, Pending)};
pmap_worker(X, Acc = #pmap_acc{n_pending=NP,
                               pending=Pending,
                               n_done=ND,
                               done=Done,
                               max_concurrent=MaxP})
  when NP == MaxP ->
    {Result, NewPending} = pmap_collect_one(Pending),
    pmap_worker(X, Acc#pmap_acc{n_pending=NP-1, pending=NewPending,
                                n_done=ND+1, done=[Result|Done]}).

%% @doc Waits for one pending pmap task to finish
pmap_collect_one(Pending) ->
    receive
        {pmap_result, Pid, Result} ->
            Size = sets:size(Pending),
            NewPending = sets:del_element(Pid, Pending),
            case sets:size(NewPending) of
                Size ->
                    pmap_collect_one(Pending);
                _ ->
                    {Result, NewPending}
            end
    end.

pmap_collect_rest(Pending, Done) ->
    case sets:size(Pending) of
        0 ->
            Done;
        _ ->
            {Result, NewPending} = pmap_collect_one(Pending),
            pmap_collect_rest(NewPending, [Result | Done])
    end.
