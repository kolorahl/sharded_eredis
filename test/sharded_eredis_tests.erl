-module(sharded_eredis_tests).

-include_lib("eunit/include/eunit.hrl").

-import(eredis, [create_multibulk/1]).

-define(Setup, fun() -> application:start(sharded_eredis)  end).
-define(Cleanup, fun(_) -> application:stop(sharded_eredis)  end).

qset(Key, Val) ->
    sharded_eredis:q(["SET", Key, Val]).
qget(Key) ->
    sharded_eredis:q(["GET", Key]).
qdel(Keys) when is_list(Keys) ->
    sharded_eredis:q(["DEL"|Keys]);
qdel(Key) ->
    sharded_eredis:q(["DEL", Key]).


basic_test_() ->
    {inparallel,

     {setup, ?Setup, ?Cleanup,
      [

       { "get and set",
         fun() ->
                 ?assertMatch({ok, _}, qdel(foo1)),
                 ?assertEqual({ok, undefined}, qget(foo1)),
                 ?assertEqual({ok, <<"OK">>}, qset(foo1, bar)),
                 ?assertEqual({ok, <<"bar">>}, qget(foo1))
         end
       },

       { "delete test",
         fun() ->
                 ?assertMatch({ok, _}, qdel(foo2)),
                 ?assertEqual({ok, <<"OK">>}, qset(foo2, bar)),
                 ?assertEqual({ok, <<"1">>}, qdel(foo2)),
                 ?assertEqual({ok, undefined}, qget(foo2))
         end
       },

       { "(c) get and set",
         fun() ->
                 ?assertMatch({ok, _}, qdel(foo3)),
                 ?assertEqual(undefined, sharded_eredis:get(foo3)),
                 ?assertEqual(ok, sharded_eredis:set(foo3, bar)),
                 ?assertEqual(<<"bar">>, sharded_eredis:get(foo3)),
                 Key = sharded_eredis:create_key([mfoo, mbar]),
                 ?assertEqual(ok, sharded_eredis:set(Key, baz)),
                 ?assertEqual(<<"baz">>, sharded_eredis:get(Key))
         end
       },

       { "(c) delete test",
         fun() ->
                 ?assertMatch({ok, _}, qdel(foo4)),
                 ?assertEqual(ok, sharded_eredis:set(foo4, bar)),
                 ?assertEqual(1, sharded_eredis:del(foo4)),
                 ?assertEqual(undefined, sharded_eredis:get(foo4))
         end
       },

       { "(c) expire test",
         fun() ->
                 ?assertMatch({ok, _}, qdel(foo5)),
                 ?assertEqual(ok, sharded_eredis:set(foo5, bar)),
                 ?assertEqual(1, sharded_eredis:expire(foo5, 1)),
                 ?assertEqual(<<"bar">>, sharded_eredis:get(foo5)),
                 ok = timer:sleep(1000),
                 ?assertEqual(undefined, sharded_eredis:get(foo5))
         end
       }
      ]
     }
    }.

