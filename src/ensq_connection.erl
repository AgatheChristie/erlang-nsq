%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @doc NSQ producer connection.
%%%
%%% Maintains a TCP connection to one nsqd for PUB/MPUB operations.
%%% Supports round-robin across multiple connections via ensq_topic.
%%%
%%% Changes from original:
%%%   - Added IDENTIFY handshake after V2 magic
%%%   - Start in passive mode, switch to active after handshake
%%%   - Improved robustness: handle empty from-queue, old sockets
%%%   - Fixed terminate crash when socket is undefined
%%% @end
%%%-------------------------------------------------------------------
-module(ensq_connection).

-behaviour(gen_server).

-include("ensq.hrl").

%% API
-export([open/3, close/1,
         start_link/3,
         send/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(CONNECT_TIMEOUT, 5000).
-define(RECV_TIMEOUT, 5000).

-record(state, {socket, buffer, topic, from = queue:new(), host, port}).

%%%===================================================================
%%% API
%%%===================================================================

open(Host, Port, Topic) ->
    ensq_connection_sup:start_child(Host, Port, Topic).

send(Pid, From, Msg) ->
    gen_server:cast(Pid, {send, From, Msg}).

close(Pid) ->
    gen_server:call(Pid, close).

start_link(Host, Port, Topic) ->
    gen_server:start_link(?MODULE, [Host, Port, Topic], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Host, Port, Topic]) ->
    TopicBin = case is_list(Topic) of
                   true -> list_to_binary(Topic);
                   false -> Topic
               end,
    {ok, connect(#state{topic = TopicBin,
                        host = Host, port = Port})}.

connect(State = #state{host = Host, port = Port}) ->
    logger:info("[conn|~s:~p] Connecting.", [Host, Port]),
    case State#state.socket of
        undefined ->
            ok;
        Old ->
            logger:info("[conn|~s:~p] Closing old socket.", [Host, Port]),
            gen_tcp:close(Old)
    end,
    Opts = [{active, false}, binary, {deliver, term}, {packet, raw}],
    State1 = State#state{socket = undefined, buffer = <<>>,
                         from = queue:new()},
    case gen_tcp:connect(Host, Port, Opts, ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            case do_handshake(Socket) of
                ok ->
                    inet:setopts(Socket, [{active, true}]),
                    logger:info("[conn|~s:~p] Connected.", [Host, Port]),
                    State1#state{socket = Socket};
                {error, Reason} ->
                    logger:error("[conn|~s:~p] Handshake failed: ~p",
                                [Host, Port, Reason]),
                    gen_tcp:close(Socket),
                    State1
            end;
        E ->
            logger:error("[conn|~s:~p] Connect error: ~p",
                        [Host, Port, E]),
            State1
    end.

%% Perform V2 magic + IDENTIFY handshake
do_handshake(Socket) ->
    case gen_tcp:send(Socket, ensq_proto:encode(version)) of
        ok ->
            case gen_tcp:send(Socket,
                              ensq_proto:encode(
                                {identify, #identity{}})) of
                ok ->
                    case recv_frame(Socket) of
                        {ok, _} -> ok;
                        {error, Reason} ->
                            {error, {identify_recv, Reason}}
                    end;
                {error, Reason} ->
                    {error, {identify_send, Reason}}
            end;
        {error, Reason} ->
            {error, {version_send, Reason}}
    end.

recv_frame(Socket) ->
    case gen_tcp:recv(Socket, 4, ?RECV_TIMEOUT) of
        {ok, <<Size:32/big>>} ->
            case gen_tcp:recv(Socket, Size, ?RECV_TIMEOUT) of
                {ok, <<FrameType:32/big, Data/binary>>} ->
                    {ok, {FrameType, Data}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({send, From, Msg}, State = #state{socket = undefined}) ->
    handle_cast({send, From, Msg}, connect(State));
handle_cast({send, From, Msg},
            State = #state{socket = S, topic = Topic, from = F}) ->
    case gen_tcp:send(S, ensq_proto:encode({publish, Topic, Msg})) of
        ok ->
            {noreply, State#state{from = queue:in(From, F)}};
        E ->
            logger:warning("[conn|~s] Send error: ~p", [Topic, E]),
            gen_server:reply(From, E),
            {noreply, connect(State)}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, S, Data},
            State = #state{socket = S, buffer = B}) ->
    State1 = data(State#state{buffer = <<B/binary, Data/binary>>}),
    {noreply, State1};
handle_info({tcp, _OtherSocket, _Data}, State) ->
    %% Data from an old/stale socket, ignore safely
    {noreply, State};
handle_info({tcp_closed, S},
            State = #state{socket = S, host = Host, port = Port}) ->
    logger:info("[conn|~s:~p] Connection closed by remote.",
               [Host, Port]),
    {noreply, connect(State)};
handle_info({tcp_closed, _OtherSocket}, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket = undefined}) ->
    ok;
terminate(_Reason, #state{socket = S}) ->
    gen_tcp:close(S),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

data(State = #state{buffer = <<Size:32/integer, Raw:Size/binary,
                                Rest/binary>>,
                    socket = S, from = F}) ->
    R =
        case Raw of
            <<0:32/integer, "_heartbeat_">> ->
                gen_tcp:send(S, ensq_proto:encode(nop)),
                ok;
            <<0:32/integer, Data/binary>> ->
                %% Response frame - match to pending PUB request
                case queue:out(F) of
                    {{value, From}, F1} ->
                        gen_server:reply(From,
                                        ensq_proto:decode(Data)),
                        {state, State#state{from = F1}};
                    {empty, _} ->
                        logger:debug("[conn] Response with no "
                                     "pending request: ~p", [Data]),
                        ok
                end;
            <<1:32/integer, Data/binary>> ->
                %% Error frame
                logger:error("[conn] NSQd error: ~p",
                            [ensq_proto:decode(Data)]),
                case queue:out(F) of
                    {{value, From}, F1} ->
                        gen_server:reply(From,
                                        {error,
                                         ensq_proto:decode(Data)}),
                        {state, State#state{from = F1}};
                    {empty, _} ->
                        ok
                end;
            <<2:32/integer, Data/binary>> ->
                %% Message frame (unexpected on PUB connection)
                case ensq_proto:decode(Data) of
                    #message{message_id = MsgID} ->
                        gen_tcp:send(S,
                                    ensq_proto:encode(
                                      {finish, MsgID}));
                    _ ->
                        ok
                end,
                ok;
            Msg ->
                logger:warning("[conn] Unknown frame: ~p", [Msg]),
                ok
        end,
    State1 = case R of
                 {state, StateX} ->
                     StateX#state{buffer = Rest};
                 _ ->
                     State#state{buffer = Rest}
             end,
    data(State1);

data(State) ->
    State.
