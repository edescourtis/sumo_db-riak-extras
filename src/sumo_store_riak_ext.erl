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
          create_schema/2
         ]
        }
       ]).

-include_lib("riakc/include/riakc.hrl").

-export([find_all/5, find_by/3, find_by/5]).

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

-spec find_all(
  sumo:schema_name(),
  term(),
  non_neg_integer(),
  non_neg_integer(),
  state()
) -> sumo_store:result([sumo_internal:doc()], state()).
find_all(DocName, _SortFields, Limit, Offset, State) ->
  %% @todo implement search with sort parameters.
  find_by(DocName, [], Limit, Offset, State).

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
find_by(DocName, Conditions, Limit, Offset,
        #state{conn = Conn,
               bucket = Bucket,
               index = Index,
               get_opts = Opts} = State) when is_list(Conditions) ->
  IdField = sumo_internal:id_field_name(DocName),
  case lists:keyfind(IdField, 1, Conditions) of
    {_K, Key} ->
      case sumo_store_riak:fetch_map(Conn, Bucket, to_bin(Key), Opts) of
        {ok, RMap} ->
          Val = sumo_store_riak:rmap_to_doc(DocName, RMap),
          {ok, [Val], State};
        {error, {notfound, _}} ->
          {ok, [], State};
        {error, Error} ->
          {error, Error, State}
      end;
    _ ->
      Query = sumo_store_riak:build_query(Conditions),
      case search_keys_by(Conn, Index, Query, Limit, Offset) of
        {ok, {_, Keys}} ->
          Results = sumo_store_riak:fetch_docs(
            DocName, Conn, Bucket, Keys, Opts),
          {ok, Results, State};
        {error, Error} -> {error, Error, State}
      end
  end;
find_by(DocName, Conditions, Limit, Offset,
        #state{conn = Conn,
               bucket = Bucket,
               index = Index,
               get_opts = Opts} = State) ->
  Query = sumo_store_riak:build_query(Conditions),
  case search_keys_by(Conn, Index, Query, Limit, Offset) of
    {ok, {_, Keys}} ->
      Results = sumo_store_riak:fetch_docs(
        DocName, Conn, Bucket, Keys, Opts),
      {ok, Results, State};
    {error, Error} -> {error, Error, State}
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
