%%%-------------------------------------------------------------------
%%% @doc ensq - Erlang NSQ client API module.
%%%
%%% Changes from original:
%%%   - Added consumer/3 for direct nsqd connection (no nsqlookupd)
%%%   - Fixed send/2 to accept list of messages (MPUB support)
%%%   - Updated touch/1 to work with gen_server channel PIDs
%%%   - Fixed typo: topic_from_sepc -> topic_from_spec
%%% @end
%%%-------------------------------------------------------------------
-module(ensq).
-export([start/0,
         init/1,
         producer/2, producer/3,
         consumer/3,
         list/0,
         send/2,
         touch/1]).


-export_type([
              host/0,
              channel/0,
              topic_name/0,
              channel_name/0
             ]).

-type host() :: {Host :: inet:ip_address() | inet:hostname(),
                 Port :: inet:port_number()}.

-type single_target() :: host().

-type multi_target() :: [host()].

-type target() :: single_target() | multi_target().

-type channel_name() ::  binary().

-type channel() :: {Channel :: channel_name(), Callback :: atom()}.

-type topic_name() :: atom() | binary().

-type topic() :: {Topic :: topic_name(), [channel()], [target()]} |
                 {Topic :: topic_name(), [channel()]}.

-type discovery_server() :: host().

-type spec() :: {[discovery_server()], [topic()]}.


start() ->
    application:start(inets),
    application:start(syntax_tools),
    application:start(compiler),
    application:start(ensq).

%%--------------------------------------------------------------------
%% @doc
%% Initialize one or more topics on a given set of discovery servers.
%% Can be called multiple times for different discovery server sets.
%%
%% Example:
%%   ensq:init({[{"127.0.0.1", 4161}], [
%%     {my_topic, [{<<"my_channel">>, my_handler}],
%%                 [{"127.0.0.1", 4150}]}
%%   ]}).
%% @end
%%--------------------------------------------------------------------
-spec init(spec()) -> ok.
init({DiscoveryServers, Topics}) ->
    [topic_from_spec(DiscoveryServers, Topic) || Topic <- Topics],
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all currently known topics.
%% @end
%%--------------------------------------------------------------------
list() ->
    ensq_topic:list().

%%--------------------------------------------------------------------
%% @doc
%% Send a message to a topic's target servers.
%% Supports both single messages (PUB) and lists of messages (MPUB).
%%
%% Examples:
%%   ensq:send(my_topic, <<"hello">>).           %% PUB
%%   ensq:send(my_topic, [<<"a">>, <<"b">>]).    %% MPUB
%% @end
%%--------------------------------------------------------------------
send(Topic, Msg) when (is_binary(Msg) orelse is_list(Msg)),
                      (is_pid(Topic) orelse is_atom(Topic)) ->
    ensq_topic:send(Topic, Msg).

%%--------------------------------------------------------------------
%% @doc
%% Create a producer connection to a single host.
%% @end
%%--------------------------------------------------------------------
-spec producer(Channel::atom()|binary(),
               Host::inet:ip_address() | inet:hostname(),
               Port::inet:port_number()) ->
                      {ok, Pid::pid()} | {error, Reason::term()}.
producer(Channel, Host, Port) ->
    producer(Channel, [{Host, Port}]).

%%--------------------------------------------------------------------
%% @doc
%% Create a producer connection to multiple hosts.
%% @end
%%--------------------------------------------------------------------
-spec producer(Channel::atom()|binary(),
               Targets :: [host()]) ->
                      {ok, Pid::pid()} | {error, Reason::term()}.
producer(Channel, Targets) ->
    ensq_topic:discover(Channel, [], [], Targets).

%%--------------------------------------------------------------------
%% @doc
%% Create a consumer that connects directly to nsqd instances.
%% No nsqlookupd required. Targets are used for both SUB and PUB.
%%
%% Example:
%%   ensq:consumer(my_topic,
%%                 [{<<"my_channel">>, my_handler_module}],
%%                 [{"127.0.0.1", 4150}]).
%%
%% The handler module must implement the ensq_channel_behaviour:
%%   -behaviour(ensq_channel_behaviour).
%%   init() -> {ok, State}.
%%   message(Msg, {Pid, MsgId}, State) -> {ok, State}.
%%   response(Msg, State) -> {ok, State}.
%%   error(Msg, State) -> {ok, State}.
%% @end
%%--------------------------------------------------------------------
-spec consumer(Topic :: topic_name(),
               Channels :: [channel()],
               NsqdHosts :: [host()]) ->
                      {ok, Pid :: pid()} | {error, Reason :: term()}.
consumer(Topic, Channels, NsqdHosts) ->
    ensq_topic:discover(Topic, [], Channels, NsqdHosts).

%%--------------------------------------------------------------------
%% @doc
%% Touch a message to reset its server-side timeout.
%% Use from within your message handler for long-running tasks.
%%
%% The argument is the {ChannelPid, MsgID} tuple passed to
%% the message/3 callback.
%%
%% Example:
%%   message(Msg, TouchRef, State) ->
%%     ensq:touch(TouchRef),
%%     %% do long work...
%%     {ok, State}.
%% @end
%%--------------------------------------------------------------------
touch({Pid, MsgID}) when is_pid(Pid) ->
    gen_server:cast(Pid, {touch, MsgID});
touch({S, MsgID}) ->
    %% Backward compatibility with raw socket
    gen_tcp:send(S, ensq_proto:encode({touch, MsgID})).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------
topic_from_spec(DiscoveryServers, {Topic, Channels}) ->
    ensq_topic:discover(Topic, DiscoveryServers, Channels);
topic_from_spec(DiscoveryServers, {Topic, Channels, []}) ->
    ensq_topic:discover(Topic, DiscoveryServers, Channels);
topic_from_spec(DiscoveryServers, {Topic, Channels, Targets}) ->
    ensq_topic:discover(Topic, DiscoveryServers, Channels, Targets).
