# RabbitStream - WIP

An Elixir Client for the the [RabbitMQ Streams](https://www.rabbitmq.com/streams.html) Protocol.

## Getting started

This project is currently under development and missing most of the features a full client needs. The current aim is to make it feature complete before writting a _Getting started_ documentation.

## Overview

The client currently consists of a GenServer process that, with the provided connection details, connects to the RabbitMQ server at its start, using [`:gen_tcp`](https://www.erlang.org/doc/man/gen_tcp.html). It then runs throught the [authentication](https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_stream/docs/PROTOCOL.adoc#authentication) sequence and mantains the connection open with heartbeats. 


## Supported Authentication Mechanisms

- "PLAIN" - `username` and `password`

## Progress
This implementation is following the protocol defined in the RabbitMQ's repository, seen [here](https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_stream/docs/PROTOCOL.adoc).

An overview of the current status of the project can be estimated by what commands it is able to process, so here is a list of the current progress of the implementation.

| Command                 | Status  | Side  |
| -                       | -       | -     |
| DeclarePublisher        |  [ ]    |       |
| Publish                 |  [ ]    |       |
| PublishConfirm          |  [ ]    |       |
| PublishError            |  [ ]    |       |
| QueryPublisherSequence  |  [ ]    |       |
| DeletePublisher         |  [ ]    |       |
| Subscribe               |  [ ]    |       |
| Deliver                 |  [ ]    |       |
| Credit                  |  [ ]    |       |
| StoreOffset             |  [ ]    |       |
| QueryOffset             |  [ ]    |       |
| Unsubscribe             |  [ ]    |       |
| Create                  |  [ ]    |       |
| Delete                  |  [ ]    |       |
| Metadata                |  [ ]    |       |
| MetadataUpdate          |  [ ]    |       |
| PeerProperties          |  [x]    |       |
| SaslHandshake           |  [x]    |       |
| SaslAuthenticate        |  [x]    |       |
| Tune                    |  [x]    |       |
| Open                    |  [x]    |       |
| Close                   |  [x]    |       |
| Heartbeat               |  [x]    |       |

## Nexts steps

After being feature complete, the next step would be to create a Broadway Producer implementation using this client.