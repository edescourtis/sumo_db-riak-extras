-module(sumo_store_riak_ext).

-behaviour(sumo_store).

-include_lib("mixer/include/mixer.hrl").
-mixin([
        {sumo_store_riak,
         [
          init/1,
          persist/2,
          delete_by/3,
          delete_all/2,
          find_by/6,
          find_all/2,
          find_all/5,
          create_schema/2
         ]
        }
       ]).

-include_lib("riakc/include/riakc.hrl").

-export([find_by/3, find_by/5, build_key/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% conn: is the Pid of the gen_server that holds the connection with Riak
%% bucket: Riak bucket (per store)
%% index: Riak index to be used by Riak Search
%% read_quorum: Riak read quorum parameters.
%% write_quorum: Riak write quorum parameters.
%% @see <a href="http://docs.basho.com/riak/latest/dev/using/basics"/>
-record(state, {conn     :: sumo_store_riak:connection(),
                bucket   :: bucket(),
                index    :: sumo_store_riak:index(),
                get_opts :: get_options(),
                put_opts :: put_options(),
                del_opts :: delete_options()}).
-type state() :: #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec find_by(
  sumo:schema_name(), sumo:conditions(), state()
) -> sumo_store:result([sumo_internal:doc()], state()).
find_by(DocName, Conditions, State) ->
  find_by(DocName, Conditions, 0, 0, State).

-spec find_by(
  sumo:schema_name(),
  sumo:conditions(),
  non_neg_integer(),
  non_neg_integer(),
  state()
) -> sumo_store:result([sumo_internal:doc()], state()).
find_by(DocName,
        Conditions,
        Limit,
        Offset,
        #state{conn = Conn,
               bucket = Bucket,
               index = Index,
               get_opts = Opts} = State) ->
  IdField = sumo_internal:id_field_name(DocName),
  case lists:keyfind(IdField, 1, Conditions) of
    {_K, Key} ->
      BinKey = iolist_to_binary(Key),
      case sumo_store_riak:fetch_map(Conn, Bucket, BinKey, Opts) of
        {ok, RMap} ->
          Val = sumo_store_riak:rmap_to_doc(DocName, RMap),
          {ok, [Val], State};
        {error, {notfound, _}} ->
          {ok, [], State};
        {error, Error} ->
          {error, Error, State}
      end;
    _ ->
      Query = build_query(Conditions),
      case search_keys_by(Conn, Index, Query, Limit, Offset) of
        {ok, {_, Keys}} ->
          Results = sumo_store_riak:fetch_docs(
            DocName, Conn, Bucket, Keys, Opts),
          {ok, Results, State};
        {error, Error} -> {error, Error, State}
      end
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
%% @doc Search all docs that match with the given query, but only keys are
%%      returned.
%%      IMPORTANT: assumes that default schema 'yokozuna' is being used.
search_keys_by(Conn, Index, Query, Limit, Offset) ->
  case sumo_store_riak:search(Conn, Index, Query, Limit, Offset) of
    {ok, {search_results, Results, _, Total}} ->
      F = fun({_, KV}, Acc) ->
            {_, K} = lists:keyfind(<<"_yz_rk">>, 1, KV),
            [K | Acc]
          end,
      Keys = lists:foldl(F, [], Results),
      {ok, {Total, Keys}};
    {error, Error} ->
      {error, Error}
  end.

%% @private
build_query([]) ->
  <<"*:*">>;
build_query(PL) when is_list(PL) ->
  build_query1(PL, <<"">>);
build_query(_) ->
  <<"*:*">>.

%% @private
to_bin(Data) when is_integer(Data) ->
  integer_to_binary(Data);
to_bin(Data) when is_float(Data) ->
  float_to_binary(Data);
to_bin(Data) when is_atom(Data) ->
  atom_to_binary(Data, utf8);
to_bin(Data) when is_list(Data) ->
  iolist_to_binary(Data);
to_bin(Data) ->
  Data.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private API - Query Builder.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
build_query1([], Acc) ->
  Acc;
build_query1([{_, [{_, _} | _T0]} = KV | T], <<"">>) ->
  build_query1(T, binary:list_to_bin(["(", build_query2(KV), ")"]));
build_query1([{_, [{_, _} | _T0]} = KV | T], Acc) ->
  build_query1(T, binary:list_to_bin([Acc, " AND (", build_query2(KV), ")"]));
build_query1([{K, V} | T], <<"">>) ->
  build_query1(T, <<(query_eq(K, V))/binary>>);
build_query1([{K, V} | T], Acc) ->
  build_query1(T, binary:list_to_bin([Acc, " AND ", query_eq(K, V)])).

%% @private
build_query2({K, [{_, _} | _T] = V}) ->
  F = fun({K_, V_}, Acc) ->
        Eq = binary:list_to_bin([build_key(K_), V_]),
        case Acc of
          <<"">> -> Eq;
          _      -> binary:list_to_bin([Acc, " ", K, " ", Eq])
        end
      end,
  lists:foldl(F, <<"">>, V).

%% @private
query_eq(K, V) ->
  binary:list_to_bin([build_key(K), V]).

%% @private
build_key(K) ->
  build_key(binary:split(to_bin(K), <<".">>, [global]), <<"">>).

%% @private
build_key([K], <<"">>) ->
  binary:list_to_bin([K, "_register:"]);
build_key([K], Acc) ->
  binary:list_to_bin([Acc, ".", K, "_register:"]);
build_key([K | T], <<"">>) ->
  build_key(T, binary:list_to_bin([K, "_map"]));
build_key([K | T], Acc) ->
  build_key(T, binary:list_to_bin([Acc, ".", K, "_map"])).
