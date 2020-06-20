%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka).

-include_lib("emqx/include/emqx.hrl").

-export([ load/1
        , unload/0
        ]).

%% Hooks functions
-export([ on_client_connected/4, on_client_disconnected/4]).

-export([ on_message_publish/2, on_message_delivered/3,on_message_acked/3]).
        

%% Called when the plugin application start
load(Env) ->
	ekaf_init([Env]),
    emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/4, [Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqx:hook('message.delivered', fun ?MODULE:on_message_delivered/3, [Env]),
    emqx:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]),
	io:format("start Test Kafka ~n",[]).

on_client_connected(#{clientid := ClientId,username := Username}, _ConnAck, _ConnInfo, _Env) ->
    %io:format("Client(~s) connected, connack: ~w~n", [ClientId, ConnAck]).
    Action = <<"connected">>,
    Now = erlang:timestamp(),
    Payload = [
    {action, Action}, 
    {device_id, ClientId}, 
    {username, Username},
    {timestamp, emqx_time:now_secs(Now)}
    ],
    produce_kafka_payload(Payload),
    ok.

on_client_disconnected(#{clientid := ClientId}, ReasonCode, _ConnInfo, _Env) ->
    %io:format("Client(~s) disconnected, reason_code: ~w~n", [ClientId, ReasonCode]).
    Action = <<"disconnected">>,
    Now = erlang:timestamp(),
    Payload = [
    {action, Action}, 
    {device_id, ClientId}, 
    {timestamp, emqx_time:now_secs(Now)}
    ],
    produce_kafka_payload(Payload),
    ok.
    
%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    %io:format("Publish ~s~n", [emqx_message:format(Message)]),
    {ok, Payload} = format_payload(Message),
    produce_kafka_payload(Payload),
    {ok, Message}.

on_message_delivered(#{clientid := ClientId}, Message, _Env) ->
    io:format("Deliver message to client(~s): ~s~n", [ClientId, emqx_message:format(Message)]),
    {ok, Message}.

on_message_acked(#{clientid := ClientId}, Message, _Env) ->
    io:format("Session(~s) acked message: ~s~n", [ClientId, emqx_message:format(Message)]),
    {ok, Message}.


ekaf_init(_Env) ->
    {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
    KafkaHost = proplists:get_value(host, BrokerValues),
    KafkaPort = proplists:get_value(port, BrokerValues),
    KafkaPartitionStrategy = proplists:get_value(partitionstrategy, BrokerValues),
    KafkaPartitionWorkers = proplists:get_value(partitionworkers, BrokerValues),
    KafkaTopic = proplists:get_value(payloadtopic, BrokerValues),
    application:set_env(ekaf, ekaf_bootstrap_broker, {KafkaHost, list_to_integer(KafkaPort)}),
    application:set_env(ekaf, ekaf_partition_strategy, list_to_atom(KafkaPartitionStrategy)),
    application:set_env(ekaf, ekaf_per_partition_workers, KafkaPartitionWorkers),
    application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(KafkaTopic)),
    application:set_env(ekaf, ekaf_buffer_ttl, 10),
    application:set_env(ekaf, ekaf_max_downtime_buffer_size, 5),
    % {ok, _} = application:ensure_all_started(kafkamocker),
    {ok, _} = application:ensure_all_started(gproc),
    % {ok, _} = application:ensure_all_started(ranch),
    {ok, _} = application:ensure_all_started(ekaf),
    io:format("start Test ekaf ~n",[]).

ekaf_get_topic() ->
    {ok, Topic} = application:get_env(ekaf, ekaf_bootstrap_topics),
    Topic.

format_payload(Message) ->
    Username = emqx_message:get_header(username, Message),
    Now = erlang:timestamp(),
    Topic = Message#message.topic,
    Tail = string:right(binary_to_list(Topic), 4),
    RawType = string:equal(Tail, <<"_raw">>),
    % io:format("Tail= ~s , RawType= ~s~n",[Tail,RawType]),

    MsgPayload = Message#message.payload,
    % io:format("MsgPayload : ~s~n", [MsgPayload]),

    if
        RawType == true ->
            MsgPayload64 = list_to_binary(base64:encode_to_string(MsgPayload));
    % io:format("MsgPayload64 : ~s~n", [MsgPayload64]);
        RawType == false ->
            MsgPayload64 = MsgPayload
    end,


    Payload = [
    	{action, message_publish},
        {device_id, Message#message.from},
        {username, Username},
        {topic, Topic},
        {payload, MsgPayload64},
        {qos,Message#message.qos},
        {ts, emqx_time:now_secs(Now)}
        ],

    {ok, Payload}.

%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/4),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqx:unhook('message.delivered', fun ?MODULE:on_message_delivered/3),
    emqx:unhook('message.acked', fun ?MODULE:on_message_acked/3).

produce_kafka_payload(Message) ->
    Topic = ekaf_get_topic(),
    {ok, MessageBody} = emqx_json:safe_encode(Message),

    % MessageBody64 = base64:encode_to_string(MessageBody),
    Payload = iolist_to_binary(MessageBody),
    ekaf:produce_async_batched(Topic, Payload),
    io:format("start Test ~n",[]).
