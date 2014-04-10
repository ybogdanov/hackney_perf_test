-module(hackney_test).

-compile([export_all]).

start() ->
  application:load(hackney_test),
  par_pool(100, 100).

seq() ->
  Res = timer:tc(?MODULE, run_seq, [10]),
  result(Res).

seq_pool() ->
  PoolName = s3,
  Options = [{timeout, 150000}, {pool_size, 50}],
  ok = hackney_pool:start_pool(PoolName, Options),

  Res = timer:tc(?MODULE, run_seq_pool, [PoolName, 10]),

  hackney_pool:stop_pool(PoolName),
  result(Res).

par(N, C) ->
  Res = timer:tc(?MODULE, run_par, [N, C]),
  result(Res).

par_pool(N, C) ->
  PoolName = s3,
  Options = [{timeout, 1000000}, {pool_size, 20}],
  ok = hackney_pool:start_pool(PoolName, Options),

  % Warm it up
  % run_par_pool(PoolName, 50, 50),

  Res = timer:tc(?MODULE, run_par_pool, [PoolName, N, C]),

  hackney_pool:stop_pool(PoolName),
  result(Res).

result({Time, Items}) ->
  Succeed = [I || I=[{version_id,_}] <- Items],

  TimeSec = Time / 1000000,
  io:format("Test finished in ~p sec (~p per sec), ~p of ~p succeed~n",
    [TimeSec, round(length(Items) / TimeSec), length(Succeed), length(Items)]),
  ok.

run_seq(0) -> [];

run_seq(N) ->
  Key = integer_to_list(N),
  Data = [integer_to_list(N), " data"],
  [put_object(Key, Data) | run_seq(N - 1)].

run_seq_pool(_PoolName, 0) -> [];

run_seq_pool(PoolName, N) ->
  Key = integer_to_list(N),
  Data = [integer_to_list(N), " data"],
  [put_object(Key, Data, PoolName) | run_seq_pool(PoolName, N - 1)].

run_par(N, P) ->
  hacknet_test_util:pmap(fun(N1) -> 
    Key = integer_to_list(N1),
    Data = [integer_to_list(N1), " data"],
    put_object(Key, Data)
  end, lists:seq(1, N), P).

run_par_pool(PoolName, N, P) ->
  hacknet_test_util:pmap(fun(N1) -> 
    Key = integer_to_binary(N1),
    Data = [integer_to_list(N1), " data"],
    put_object(Key, Data, PoolName)
  end, lists:seq(1, N), P).

put_object(Key, Data) -> put_object(Key, Data, none).

put_object(Key, Data, PoolName) ->
  Key1 = <<"perf_test/", Key/binary>>,

  Options = [
    % {acl, private}
  ],

  Options1 = case PoolName of
    none -> Options;
    Other -> [{pool, Other} | Options]
  end,

  % io:format("Options: ~p~n", [Options1]),

  HTTPHeaders = [
    {<<"content-type">>, <<"text/plain">>}
  ],

  {Time, Res} = timer:tc(?MODULE, do_req, [<<"hackney_test">>, Key1, Data, Options1, HTTPHeaders]) ,

  io:format("Req took: ~p sec~n", [Time / 1000000]),
  Res.

do_req(Bucket, Key, Data, Options, HTTPHeaders) ->
  URL = <<"https://requestb.in/1iz4ev71">>,
  {ok, _StatusCode, _RespHeaders, ClientRef} = hackney:request(put, URL, HTTPHeaders, Data, Options),
  {ok, R} = hackney:body(ClientRef),
  [{version_id,1}].
  % s3:configure(
  %   application:get_env(hackney_test, aws_key, undefined), 
  %   application:get_env(hackney_test, aws_secret, undefined)
  % ),
  % R = (catch s3:put_object(Bucket, Key, Data, Options, HTTPHeaders)),
  % dump_error(R),
  % R.

dump_error({'EXIT', Error}) ->
  io:format("Error: ~p~n", [Error]);

dump_error(_) -> ok.
