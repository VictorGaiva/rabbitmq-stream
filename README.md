# RabbitMQStream - WIP

[![Version](https://img.shields.io/hexpm/v/rabbitmq_stream.svg)](https://hex.pm/packages/rabbitmq_stream)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/rabbitmq_stream/)
[![Download](https://img.shields.io/hexpm/dt/rabbitmq_stream.svg)](https://hex.pm/packages/rabbitmq_stream)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Elixir Client for [RabbitMQ Streams Protocol](https://www.rabbitmq.com/streams.html).

## Usage

### RabbitMQStream.Publisher

A publisher module can be defined like this:

```elixir
defmodule MyApp.MyPublisher do
  use RabbitMQStream.Publisher,
    stream_name: "my-stream"
end
```

After adding it to your supervision tree, you can publish messages with:

```elixir
MyApp.MyPublisher.publish("Hello, world!")
```

For more information, check the [documentation](https://hexdocs.pm/rabbitmq_stream/)

## Status

The current aim is to fully document the already implemented features, which are the `RabbitMQStream.Connection`, `RabbitMQStream.Publisher` and `RabbitMQStream.SupervisedPublisher`. Then the next step will be to implement the `RabbitMQStream.Subscriber` and `RabbitMQStream` Integerated Client.

## Progress

This implementation is following the protocol defined in the RabbitMQ's repository, seen [here](https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_stream/docs/PROTOCOL.adoc).

Here is the set of messages with handlers currently implemented:

| Command                | Status | Description                               |
| ---------------------- | ------ | ----------------------------------------- |
| DeclarePublisher       | ✅     | --                                        |
| Publish                | ✅     |
| PublishConfirm         | ✅     |
| PublishError           | ✅     |
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

## Roadmap

- [_Well-behaved_](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/#:~:text=Well%2Dbehaved%20Clients) Cluster connection
- Workaround for connecting throught [load balancers](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams#:~:text=Client%20Workaround%20With%20a%20Load%20Balancer)

## Future Plans

- Broadway Producer implementation using this client.
