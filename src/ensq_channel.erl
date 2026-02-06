%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @doc NSQ channel consumer - rewritten as gen_server
%%%
%%% Each channel process maintains a TCP connection to one nsqd,
%%% subscribes to a topic/channel, and dispatches messages to
%%% the configured handler module (ensq_channel_behaviour).
%%%
%%% Changes from original:
%%%   - Rewritten as proper gen_server (was spawn+receive loop)
%%%   - Added IDENTIFY handshake after V2 magic
%%%   - Proper OTP supervision support
%%%   - Touch via gen_server:cast instead of raw socket
%%% @end
%%%-------------------------------------------------------------------
-module(ensq_channel).

-behaviour(gen_server).

-include("ensq.hrl").

%% API
-export([open/5, ready/2, close/1, start_link/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(FRAME_TYPE_RESPONSE, 0).
-define(FRAME_TYPE_ERROR, 1).
-define(FRAME_TYPE_MESSAGE, 2).
-define(CONNECT_TIMEOUT, 5000).
-define(RECV_TIMEOUT, 5000).

-record(state, {
          socket :: gen_tcp:socket() | undefined,
          buffer = <<>> :: binary(),
          current_ready_count = 1 :: non_neg_integer(),
          ready_count = 1 :: non_neg_integer(),
          handler = ensq_debug_callback :: atom(),
          cstate :: term(),
          host :: inet:ip_address() | inet:hostname(),
          port :: inet:port_number(),
          topic :: binary(),
          channel :: binary()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Open a new channel connection via the channel supervisor.
open(Host, Port, Topic, Channel, Handler) ->
    ensq_channel_sup:start_child(Host, Port, Topic, Channel, Handler).

%% @doc Update the RDY count for this channel.
ready(Pid, N) ->
    gen_server:cast(Pid, {ready, N}).

%% @doc Close this channel connection.
close(Pid) ->
    gen_server:cast(Pid, close).

%% @doc Start and link this channel process (called by supervisor).
start_link(Host, Port, Topic, Channel, Handler) ->
    gen_server:start_link(?MODULE, [Host, Port, Topic, Channel, Handler], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Host, Port, Topic, Channel, Handler]) ->
    logger:debug("[channel|~s:~p] connecting", [Host, Port]),
    Opts = [{active, false}, binary, {deliver, term}, {packet, raw}],
    case gen_tcp:connect(Host, Port, Opts, ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            case do_handshake(Socket, Topic, Channel) of
                ok ->
                    inet:setopts(Socket, [{active, true}]),
                    ok = gen_tcp:send(Socket,
                                     ensq_proto:encode({ready, 1})),
                    {ok, CState} = Handler:init(),
                    logger:info("[channel|~s:~p] subscribed ~s/~s",
                               [Host, Port, Topic, Channel]),
                    ensq_in_flow_manager:getrc(),
                    {ok, #state{
                            socket = Socket,
                            handler = Handler,
                            cstate = CState,
                            host = Host,
                            port = Port,
                            topic = Topic,
                            channel = Channel
                           }};
                {error, Reason} ->
                    gen_tcp:close(Socket),
                    logger:error("[channel|~s:~p] handshake failed: ~p",
                                [Host, Port, Reason]),
                    {stop, Reason}
            end;
        {error, Reason} ->
            logger:error("[channel|~s:~p] connect failed: ~p",
                        [Host, Port, Reason]),
            {stop, Reason}
    end.

handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ready, 0}, State) ->
    {noreply, State#state{ready_count = 0, current_ready_count = 0}};
handle_cast({ready, N}, State = #state{socket = Socket}) ->
    gen_tcp:send(Socket, ensq_proto:encode({ready, N})),
    {noreply, State#state{ready_count = N, current_ready_count = N}};
handle_cast({touch, MsgID}, State = #state{socket = Socket}) ->
    gen_tcp:send(Socket, ensq_proto:encode({touch, MsgID})),
    {noreply, State};
handle_cast(close, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data},
            State = #state{socket = Socket, buffer = Buffer}) ->
    NewBuffer = <<Buffer/binary, Data/binary>>,
    NewState = process_data(State#state{buffer = NewBuffer}),
    NewState2 = maybe_replenish_rdy(NewState),
    {noreply, NewState2};

handle_info({tcp_closed, Socket},
            State = #state{socket = Socket, host = Host, port = Port}) ->
    logger:info("[channel|~s:~p] connection closed by remote",
               [Host, Port]),
    {stop, normal, State#state{socket = undefined}};

handle_info({tcp_error, Socket, Reason},
            State = #state{socket = Socket, host = Host, port = Port}) ->
    logger:error("[channel|~s:~p] tcp error: ~p",
                [Host, Port, Reason]),
    {stop, {tcp_error, Reason}, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket = undefined}) ->
    ok;
terminate(_Reason, #state{socket = Socket}) ->
    catch gen_tcp:send(Socket, ensq_proto:encode(close)),
    gen_tcp:close(Socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal - Handshake
%%%===================================================================

%% @doc NSQ handshake: V2 magic -> IDENTIFY -> SUB
do_handshake(Socket, Topic, Channel) ->
    %% 1. Send V2 magic
    ok = gen_tcp:send(Socket, ensq_proto:encode(version)),
    %% 2. Send IDENTIFY
    ok = gen_tcp:send(Socket,
                      ensq_proto:encode({identify, #identity{}})),
    %% 3. Read IDENTIFY response
    case recv_frame(Socket) of
        {ok, {?FRAME_TYPE_RESPONSE, _IdentifyData}} ->
            %% 4. Subscribe to topic/channel
            ok = gen_tcp:send(Socket,
                              ensq_proto:encode({subscribe,
                                                 Topic, Channel})),
            %% 5. Read SUB acknowledgment
            case recv_frame(Socket) of
                {ok, {?FRAME_TYPE_RESPONSE, _}} ->
                    ok;
                {ok, {?FRAME_TYPE_ERROR, ErrData}} ->
                    {error, {subscribe_error, ErrData}};
                {error, Reason} ->
                    {error, {subscribe_failed, Reason}}
            end;
        {ok, {?FRAME_TYPE_ERROR, ErrData}} ->
            {error, {identify_error, ErrData}};
        {error, Reason} ->
            {error, {identify_failed, Reason}}
    end.

%% @doc Read one NSQ frame from socket in passive mode.
%% Frame: [Size:4-BE][FrameType:4-BE][Data:Size-4 bytes]
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

%%%===================================================================
%%% Internal - Data Processing
%%%===================================================================

%% @doc Process all complete frames in the TCP buffer.
%% Replies are accumulated and sent in a single TCP write (batch).
process_data(State = #state{buffer = Buffer, socket = Socket,
                            current_ready_count = CRC,
                            handler = Handler, cstate = CState}) ->
    {NewBuffer, NewCRC, NewCState, Replies} =
        process_frames(Buffer, CRC, Handler, CState, self(), <<>>),
    case byte_size(Replies) of
        0 -> ok;
        _ -> gen_tcp:send(Socket, Replies)
    end,
    State#state{buffer = NewBuffer, current_ready_count = NewCRC,
                cstate = NewCState}.

%% @doc Recursively extract and handle frames from buffer.
process_frames(<<Size:32/big, Raw:Size/binary, Rest/binary>>,
               RC, Handler, CState, Self, Replies) ->
    {Reply, NewCState} =
        handle_raw_frame(Raw, Handler, CState, Self),
    process_frames(Rest, RC - 1, Handler, NewCState, Self,
                   <<Replies/binary, Reply/binary>>);
process_frames(Rest, RC, _Handler, CState, _Self, Replies) ->
    {Rest, RC, CState, Replies}.

%% Message frame
handle_raw_frame(<<?FRAME_TYPE_MESSAGE:32, _Ts:64, _Att:16,
                   MsgID:16/binary, Msg/binary>>,
                 Handler, CState, Self) ->
    case Handler:message(Msg, {Self, MsgID}, CState) of
        {ok, CState1} ->
            {ensq_proto:encode({finish, MsgID}), CState1};
        {requeue, CState1, Timeout} ->
            {ensq_proto:encode({requeue, MsgID, Timeout}), CState1}
    end;
%% Heartbeat
handle_raw_frame(<<?FRAME_TYPE_RESPONSE:32, "_heartbeat_">>,
                 _Handler, CState, _Self) ->
    {ensq_proto:encode(nop), CState};
%% Other response
handle_raw_frame(<<?FRAME_TYPE_RESPONSE:32, Msg/binary>>,
                 Handler, CState, _Self) ->
    {ok, CState1} = Handler:response(ensq_proto:decode(Msg), CState),
    {<<>>, CState1};
%% Error frame (protocol-level errors like E_INVALID, E_BAD_TOPIC, etc.)
handle_raw_frame(<<?FRAME_TYPE_ERROR:32, ErrData/binary>>,
                 Handler, CState, _Self) ->
    case Handler:error(ErrData, CState) of
        {ok, CState1} ->
            {<<>>, CState1};
        {error, _Reason} ->
            %% Handler returned error, keep original state
            {<<>>, CState}
    end;
%% Unknown frame
handle_raw_frame(Unknown, _Handler, CState, _Self) ->
    logger:warning("[channel] unknown frame: ~p", [Unknown]),
    {<<>>, CState}.

%%%===================================================================
%%% Internal - RDY Management
%%%===================================================================

%% @doc Replenish RDY when current count drops below 25% of target.
maybe_replenish_rdy(State = #state{current_ready_count = CRC,
                                   ready_count = RC,
                                   socket = Socket})
  when RC > 0, CRC * 4 < RC ->
    %% Occasionally re-negotiate with flow manager
    case rand:uniform(10) of
        10 -> ensq_in_flow_manager:getrc();
        _ -> ok
    end,
    gen_tcp:send(Socket, ensq_proto:encode({ready, RC})),
    State#state{current_ready_count = RC};
maybe_replenish_rdy(State) ->
    State.
