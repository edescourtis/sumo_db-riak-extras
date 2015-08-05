PROJECT = sumo_store_riak_ext

CONFIG ?= test/test.config

DEPS = mixer sumo_db katana

dep_mixer   = git git://github.com/inaka/mixer.git     0.1.3
dep_sumo_db = git git://github.com/inaka/sumo_db.git   0.3.8
dep_katana  = git git://github.com/inaka/erlang-katana 0.2.5

DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Werror_handling \
                 -Wrace_conditions #-Wunmatched_returns

include erlang.mk

ERLC_OPTS += +'{parse_transform, lager_transform}' +debug_info

TEST_ERLC_OPTS += +'{parse_transform, lager_transform}' +debug_info
CT_SUITES = sumo_store_riak
CT_OPTS = -cover test/sumo.coverspec -erl_args -config ${CONFIG}

SHELL_OPTS = -name ${PROJECT}@`hostname` -s ${PROJECT} -config ${CONFIG} -s sync

test-shell: build-ct-suites app
	erl -pa ebin -pa deps/*/ebin -pa test -s lager -s sync -config ${CONFIG}

devtests: tests
	open logs/index.html

quicktests: app build-ct-suites
	@if [ -d "test" ] ; \
	then \
		mkdir -p logs/ ; \
		$(CT_RUN) -suite $(addsuffix _SUITE,$(CT_SUITES)) $(CT_OPTS) ; \
	fi
	$(gen_verbose) rm -f test/*.beam

erldocs: app
	erldocs . -o doc/
