-module(s3).

-export([configure/2, get_object/3, put_object/5]).

-record(s3_config, {
  s3_scheme             = <<"https://">>             :: binary(),
  s3_host               = <<"s3.amazonaws.com">>     :: binary(),
  s3_port               = 80                         :: non_neg_integer(),
  access_key_id                                      :: binary() | undefined | false,
  secret_access_key                                  :: binary() | undefined | false,
  security_token        = undefined                  :: binary() | undefined,
  timeout               = 10000                      :: non_neg_integer()
}).

configure(AccessKeyID, SecretAccessKey) ->
  put(s3_config, #s3_config{
    access_key_id = AccessKeyID,
    secret_access_key = SecretAccessKey
  }).

-spec get_object(binary(), binary(), proplists:proplist()) -> proplists:proplist().

get_object(BucketName, Key, Options) ->
    Config = get(s3_config),

    RequestHeaders = [{<<"Range">>, proplists:get_value(range, Options)},
                      {<<"If-Modified-Since">>, proplists:get_value(if_modified_since, Options)},
                      {<<"If-Unmodified-Since">>, proplists:get_value(if_unmodified_since, Options)},
                      {<<"If-Match">>, proplists:get_value(if_match, Options)},
                      {<<"If-None-Match">>, proplists:get_value(if_none_match, Options)}],
    Subresource = case proplists:get_value(version_id, Options) of
                      undefined -> "";
                      Version   -> ["versionId=", Version]
                  end,
    {Headers, Body} = s3_request(Config, get, BucketName, <<"/", Key/binary>>, Subresource, [], <<>>, RequestHeaders, Options),
    [{etag, proplists:get_value(<<"etag">>, Headers)},
     {content_length, proplists:get_value(<<"content-length">>, Headers)},
     {content_type, proplists:get_value(<<"content-type">>, Headers)},
     {delete_marker, list_to_existing_atom(binary_to_list(proplists:get_value(<<"x-amz-delete-marker">>, Headers, <<"false">>)))},
     {version_id, proplists:get_value(<<"x-amz-version-id">>, Headers, <<"null">>)},
     {content, Body}|
     extract_metadata(Headers)].

put_object(BucketName, Key, Value, Options, HTTPHeaders)
  when is_binary(BucketName), is_binary(Key), is_list(Value) orelse is_binary(Value),
       is_list(Options) ->

    Config = get(s3_config),

    RequestHeaders = [
      {<<"x-amz-acl">>, encode_acl(proplists:get_value(acl, Options))} | HTTPHeaders
    ] ++ [
      % TODO: MKey should be binary
      {iolist_to_binary([<<"x-amz-meta-">>, lower(MKey)]), MValue}
        || {MKey, MValue} <- proplists:get_value(meta, Options, [])
    ],

    {RequestHeaders2, ContentType} = case proplists:get_value(<<"content-type">>, HTTPHeaders) of
      undefined ->
        Default = <<"application/octet_stream">>,
        {[{<<"content-type">>, Default} | RequestHeaders], Default};
      Other ->
        {RequestHeaders, Other}
    end,

    POSTData = {iolist_to_binary(Value), ContentType},
    {Headers, _Body} = s3_request(Config, put, BucketName, <<"/", Key/binary>>, "", [], POSTData, RequestHeaders2, Options),
    [{version_id, proplists:get_value(<<"x-amz-version-id">>, Headers, <<"null">>)}].

encode_acl(undefined)                 -> undefined;
encode_acl(private)                   -> <<"private">>;
encode_acl(public_read)               -> <<"public-read">>;
encode_acl(public_read_write)         -> <<"public-read-write">>;
encode_acl(authenticated_read)        -> <<"authenticated-read">>;
encode_acl(bucket_owner_read)         -> <<"bucket-owner-read">>;
encode_acl(bucket_owner_full_control) -> <<"bucket-owner-full-control">>.

lower(Str) when is_list(Str) -> list_to_binary(string:to_lower(Str));
lower(Str) when is_binary(Str) -> lower(binary_to_list(Str)).

extract_metadata(Headers) ->
    [{Key, Value} || {<<"x-amz-meta-", Key/binary>>, Value} <- Headers].

s3_request(Config, Method, BucketName, Path, Subresource, Params, POSTData, Headers, Options) ->
    case s3_request2(Config, Method, BucketName, Path, Subresource, Params, POSTData, Headers, Options) of
        {ok, Result} -> Result;
        {error, Reason} -> erlang:error({aws_error, Reason})
    end.

s3_request2(Config, Method, BucketName, Path, Subresource, Params, POSTData, Headers0, Options) ->
    {ContentMD5, ContentType, Body} =
        case POSTData of
            {PD, CT} -> {base64:encode(crypto:hash(md5, PD)), CT, PD};
            PD -> {"", "", PD}
        end,

    Headers = case Config#s3_config.security_token of
                  undefined -> Headers0;
                  Token when is_binary(Token) -> [{<<"x-amz-security-token">>, Token} | Headers0]
              end,

    FHeaders = [Header || {_, Value} = Header <- Headers, Value =/= undefined],
    AmzHeaders = [Header || {<<"x-amz-", _/binary>>, _} = Header <- FHeaders],
    Date = httpd_util:rfc1123_date(erlang:localtime()),
    EscapedPath = url_encode_loose(Path),
    Authorization = make_authorization(Config, Method, ContentMD5, ContentType,
                                       Date, AmzHeaders, BucketName, EscapedPath, Subresource),
    RequestHeaders = [
      {<<"date">>, list_to_binary(Date)},
      {<<"authorization">>, Authorization}|FHeaders
    ] ++ case ContentMD5 of
      "" -> [];
      _ -> [{<<"content-md5">>, ContentMD5}]
    end,

    RequestURI = iolist_to_binary([
                                Config#s3_config.s3_scheme,
                                case BucketName of "" -> ""; _ -> [BucketName, $.] end,
                                Config#s3_config.s3_host, port_spec(Config),
                                EscapedPath,
                                case Subresource of "" -> ""; _ -> [$?, Subresource] end,
                                if
                                    Params =:= [] -> "";
                                    true -> [$&, hackney_url:qs(Params)]
                                end
                               ]),

    % extract hackney options
    HackneyOptionsKeys = [pool],
    HackneyOptions = [{K, V} || {K, V} <- Options, lists:member(K, HackneyOptionsKeys)],

    % io:format("Headers: ~p~n", [RequestHeaders]),
    % io:format("Request: ~p ~p ~p ~p ~p~n", [Method, RequestURI, RequestHeaders, Body, HackneyOptions]),

    Response = hackney:request(Method, RequestURI, RequestHeaders, Body, HackneyOptions),
    http_headers_body(Response).

make_authorization(Config, Method, ContentMD5, ContentType, Date, AmzHeaders, BucketName, Resource, Subresource) ->
    CanonizedAmzHeaders =
        [[Name, $:, Value, $\n] || {Name, Value} <- lists:sort(AmzHeaders)],
    StringToSign = [string:to_upper(atom_to_list(Method)), $\n,
                    ContentMD5, $\n,
                    ContentType, $\n,
                    Date, $\n,
                    CanonizedAmzHeaders,
                    case BucketName of "" -> ""; _ -> [$/, BucketName] end,
                    Resource,
                    case Subresource of "" -> ""; _ -> [$?, Subresource] end
                   ],
    Signature = base64:encode(crypto:hmac(sha, Config#s3_config.secret_access_key, StringToSign)),
    ["AWS ", Config#s3_config.access_key_id, $:, Signature].

-type headers() :: [{binary(), binary()}].
-spec http_headers_body({ok, tuple()} | {error, term()}) -> {ok, {headers(), term()}} | {error, tuple()}.
%% Extract the headers and body and do error handling on the return of a httpc:request call.
http_headers_body({ok, OKStatus, Headers, Client}) 
  when OKStatus >= 200, OKStatus =< 299 ->
    case hackney:body(Client) of
      {ok, Body} ->
        {ok, {Headers, Body}};
      {error, Reason} ->
        {error, Reason}
    end;
http_headers_body({ok, Status, _Headers, Client}) ->
  case hackney:body(Client) of
    {ok, Body} ->
      {error, {http_error, Status, Body}};
    _ ->
      {error, {http_error, Status, <<>>}}
  end;
http_headers_body({error, Reason}) ->
    {error, {socket_error, Reason}}.

url_encode_loose(Binary) when is_binary(Binary) ->
    url_encode_loose(binary_to_list(Binary));
url_encode_loose(String) ->
    url_encode_loose(String, []).
url_encode_loose([], Accum) ->
    lists:reverse(Accum);
url_encode_loose([Char|String], Accum)
  when Char >= $A, Char =< $Z;
       Char >= $a, Char =< $z;
       Char >= $0, Char =< $9;
       Char =:= $-; Char =:= $_;
       Char =:= $.; Char =:= $~;
       Char =:= $/; Char =:= $: ->
    url_encode_loose(String, [Char|Accum]);
url_encode_loose([Char|String], Accum)
  when Char >=0, Char =< 255 ->
    url_encode_loose(String, [hex_char(Char rem 16), hex_char(Char div 16),$%|Accum]).

hex_char(C) when C >= 0, C =< 9 -> $0 + C;
hex_char(C) when C >= 10, C =< 15 -> $A + C - 10.

port_spec(#s3_config{s3_port=80}) ->
    <<>>;
port_spec(#s3_config{s3_port=Port}) ->
    iolist_to_binary([":", erlang:integer_to_list(Port)]).
