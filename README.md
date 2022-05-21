# RabbitStream - WIP

Elixir Client for [RabbitMQ Streams Protocol](https://www.rabbitmq.com/streams.html).

## Getting started

This project is currently under development and missing most of the features a full client needs. The current aim is to make it feature complete before writting a _Getting started_ documentation.

## Overview

### `Connection`

Responsible for encoding and decoding messages, opening and maintaining a socket connection to a single node.
It connects to the RabbitMQ server using [`:gen_tcp`](https://www.erlang.org/doc/man/gen_tcp.html). It then runs throught the [authentication](https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_stream/docs/PROTOCOL.adoc#authentication) sequence and mantains the connection open with heartbeats, with the provided `tune` definition.

### `Client`

Responsible for managing multiple connections for a single cluster, routing requests accordingly, and enforcing [_Well-behaved_](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/) praticies.

### `Publisher`

--

### Supported Authentication Mechanisms

- `PLAIN`

## Progress

This implementation is following the protocol defined in the RabbitMQ's repository, seen [here](https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_stream/docs/PROTOCOL.adoc).

An overview of the current status of the project can be estimated by what commands it is able to process, so here is a list of the current progress of the implementation.

| Command                | Status | Description                               |
| ---------------------- | ------ | ----------------------------------------- |
| DeclarePublisher       | ✅     | --                                        |
| Publish                | ⏳     |
| PublishConfirm         | ⏳     |
| PublishError           | ⏳     |
| QueryPublisherSequence | ✅     | --                                        |
| DeletePublisher        | ✅     | --                                        |
| Subscribe              | ⏳     |
| Deliver                | ⏳     |
| Credit                 | ⏳     |
| StoreOffset            | ✅     | Stores a stream offset under given `name` |
| QueryOffset            | ✅     | Retrieves a stored offset                 |
| Unsubscribe            | ⏳     |
| Create                 | ✅     | Create a Stream                           |
| Delete                 | ✅     | Delete a Stream                           |
| Metadata               | ✅     | --                                        |
| MetadataUpdate         | ✅     | --                                        |
| PeerProperties         | ✅     | --                                        |
| SaslHandshake          | ✅     | --                                        |
| SaslAuthenticate       | ✅     | --                                        |
| Tune                   | ✅     | --                                        |
| Open                   | ✅     | --                                        |
| Close                  | ✅     | --                                        |
| Heartbeat              | ✅     | --                                        |

## Nexts steps

After being feature complete, the next step would be to create a Broadway Producer implementation using this client.

## Requirements for final release

- [_Well-behaved_](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/#:~:text=Well%2Dbehaved%20Clients) Cluster connection
- Workaround for connecting throught [load balancers](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams#:~:text=Client%20Workaround%20With%20a%20Load%20Balancer)
