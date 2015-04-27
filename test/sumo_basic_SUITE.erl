-module(sumo_basic_SUITE).

-export([
         all/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2
        ]).

-export([
         find_all/1,
         find_by/1,
         delete_all/1,
         delete/1
        ]).

-type config() :: [{atom(), term()}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Common test
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec all() -> [atom()].
all() ->
  [find_all, find_by, delete_all, delete].

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
  application:ensure_all_started(sumo_db),
  Config.

init_per_testcase(_, Config) ->
  init_store(),
  Config.

-spec end_per_suite(config()) -> config().
end_per_suite(Config) ->
  sumo:delete_all(sumo_test_purchase_order),
  Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Exported Tests Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

find_all(_Config) ->
  3 = length(sumo:find_all(sumo_test_purchase_order)).

find_by(_Config) ->
  Results1 = sumo:find_by(sumo_test_purchase_order, [{currency, <<"USD">>}]),
  2 = length(Results1),

  [
    #{id := <<"ID1">>,
      currency := <<"USD">>,
      items := [#{part_num := <<"123">>}, #{part_num := <<"456">>}],
      order_num := <<"O1">>,
      ship_to := #{city := <<"city">>, country := <<"US">>},
      bill_to := #{city := <<"city">>, country := <<"US">>},
      total := 300},
    #{id := <<"ID2">>,
      currency := <<"USD">>,
      items := [#{part_num := <<"123">>}, #{part_num := <<"456">>}],
      order_num := <<"O2">>,
      ship_to := #{city := <<"city">>, country := <<"US">>},
      bill_to := #{city := <<"city">>, country := <<"US">>},
      total := 300}
  ] = Results1,

  Results2 = sumo:find_by(sumo_test_purchase_order, [{currency, <<"EUR">>}]),
  1 = length(Results2),

  [
    #{id := <<"ID3">>,
      currency := <<"EUR">>,
      items := [#{part_num := <<"123">>}, #{part_num := <<"456">>}],
      order_num := <<"O3">>,
      ship_to := #{city := <<"city">>, country := <<"US">>},
      bill_to := #{city := <<"city">>, country := <<"US">>},
      total := 300}
  ] = Results2,

  PO1 = sumo:find(sumo_test_purchase_order, <<"ID1">>),
  #{id := <<"ID1">>,
    currency := <<"USD">>,
    items := [#{part_num := <<"123">>}, #{part_num := <<"456">>}],
    order_num := <<"O1">>,
    ship_to := #{city := <<"city">>, country := <<"US">>},
    bill_to := #{city := <<"city">>, country := <<"US">>},
    total := 300} = PO1,

  notfound = sumo:find(sumo_test_purchase_order, <<"ID123">>),

  ok.

delete_all(_Config) ->
  sumo:delete_all(sumo_test_purchase_order),
  [] = sumo:find_all(sumo_test_purchase_order).

delete(_Config) ->
  %% delete_by
  2 = sumo:delete_by(sumo_test_purchase_order, [{currency, <<"USD">>}]),
  timer:sleep(5000),
  Results = sumo:find_by(sumo_test_purchase_order, [{currency, <<"USD">>}]),

  0 = length(Results),

  %% delete
  sumo:delete(sumo_test_purchase_order, <<"ID3">>),
  0 = length(sumo:find_all(sumo_test_purchase_order)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_store() ->
  sumo:create_schema(sumo_test_purchase_order),
  sumo:delete_all(sumo_test_purchase_order),

  Addr = sumo_test_purchase_order:new_address(
    <<"line1">>, <<"line2">>, <<"city">>, <<"state">>, <<"zip">>, <<"US">>),
  Item1 = sumo_test_purchase_order:new_item(<<"123">>, <<"p1">>, 1, 100, 100),
  Item2 = sumo_test_purchase_order:new_item(<<"456">>, <<"p2">>, 2, 100, 200),
  Items = [Item1, Item2],
  Date = iolist_to_binary(httpd_util:rfc1123_date(calendar:local_time())),
  PO1 = sumo_test_purchase_order:new(
    <<"ID1">>, <<"O1">>, Date, Addr, Addr, Items, <<"USD">>, 300),
  PO2 = sumo_test_purchase_order:new(
    <<"ID2">>, <<"O2">>, Date, Addr, Addr, Items, <<"USD">>, 300),
  PO3 = sumo_test_purchase_order:new(
    <<"ID3">>, <<"O3">>, Date, Addr, Addr, Items, <<"EUR">>, 300),

  sumo:persist(sumo_test_purchase_order, PO1),
  sumo:persist(sumo_test_purchase_order, PO2),
  sumo:persist(sumo_test_purchase_order, PO3),

  %% Sync Timeout.
  timer:sleep(5000).
