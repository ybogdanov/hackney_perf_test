#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable

-mode(compile).

main([Ns, Cs]) ->
  [code:add_path(P) || P <- filelib:wildcard("./deps/*/ebin")],
  hackney:start(),

  N = list_to_integer(Ns),
  C = list_to_integer(Cs),

  PoolName = test,
  Options = [{timeout, 1000000}, {pool_size, C}],
  ok = hackney_pool:start_pool(PoolName, Options),

  {Time, Items} = timer:tc(fun run_par_pool/2, [PoolName, N]),

  hackney_pool:stop_pool(PoolName),
  Succeed = length([1 || {ok,_} <- Items]),

  TimeSec = Time / 1000000,
  io:format("Test finished in ~p sec (~p per sec), ~p of ~p succeed~n",
    [TimeSec, round(length(Items) / TimeSec), Succeed, length(Items)]),
  ok.

run_par_pool(PoolName, N) ->
  pmap(fun(N1) -> 
    Data = [integer_to_list(N1), " data"],
    put_object(Data, PoolName)
  end, lists:seq(1, N)).

put_object(Data, PoolName) ->
  Options = case PoolName of
    none -> [];
    Other -> [{pool, Other}]
  end,

  HTTPHeaders = [
    {<<"content-type">>, <<"text/plain">>}
  ],

  {Time, Res} = timer:tc(fun do_req/3, [Data, Options, HTTPHeaders]) ,

  io:format("Req took: ~p sec~n", [Time / 1000000]),
  Res.

do_req(Data, Options, HTTPHeaders) ->
  URL = <<"https://requestb.in/1iz4ev71">>,
  {ok, _StatusCode, _RespHeaders, ClientRef} = hackney:request(put, URL, HTTPHeaders, Data, Options),
  hackney:body(ClientRef).

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
