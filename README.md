# RabbitMQStream

[![Version](https://img.shields.io/hexpm/v/rabbitmq_stream.svg)](https://hex.pm/packages/rabbitmq_stream)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/rabbitmq_stream/)
[![Download](https://img.shields.io/hexpm/dt/rabbitmq_stream.svg)](https://hex.pm/packages/rabbitmq_stream)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Unit Tests](https://github.com/VictorGaiva/rabbitmq-stream/actions/workflows/ci.yaml/badge.svg)](https://github.com/VictorGaiva/rabbitmq-stream/actions)

Elixir Client for [RabbitMQ Streams Protocol](https://www.rabbitmq.com/streams.html).

## Overview

RabbiMQ 3.9 introduced the [Streams](https://www.youtube.com/watch?v=PnmGoMiaJhE) as an alternative to Queues, but differs mainly by implementing a ["non-destructive consumer semantics"](https://www.rabbitmq.com/docs/streams#overview). A consumer can read the messages starting at any offset, while receiving new messages.

While this feature is avaiable when using the existing Queues, it shines when used with its dedicated protocol, that allows messages to be consumed [extremelly fast](https://youtu.be/PnmGoMiaJhE?si=oHBaa6ml1dGewuvT&t=1125), in comparisson to Queues.

This library aims to be a Client for the [Streams Protocol](https://www.rabbitmq.com/docs/stream), managing connections and providing an idiomatic way of interacting with all the features avaiable for this functionallity.

## Features

- Producing and Consuming from Streams
- [Offset Tracking](https://www.rabbitmq.com/blog/2021/09/13/rabbitmq-streams-offset-tracking) and [Control Flow](https://www.rabbitmq.com/docs/stream#flow-control)(Credits) helpers
- [Stream Filtering](https://www.rabbitmq.com/blog/2023/10/16/stream-filtering)
- [Single Active Consumer](https://www.rabbitmq.com/blog/2022/07/05/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams)
- [Super Streams](https://www.rabbitmq.com/blog/2022/07/13/rabbitmq-3-11-feature-preview-super-streams)

## Installation

The package can be installed by adding `rabbitmq_stream` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:rabbitmq_stream, "~> 0.4.2"},
    # ...
  ]
end
```

## [Producing](https://github.com/VictorGaiva/rabbitmq-stream/blob/main/guides/concepts/producing.md)

RabbitMQ Streams protocol needs a static `:reference_name` per producer. This is used to prevent message duplication. For this reason, each stream needs, for now, a static module to publish messages, which keeps track of its own `publishing_id`.

You can define a `Producer` module like this:

```elixir
defmodule MyApp.MyProducer do
  use RabbitMQStream.Producer,
    stream_name: "stream-01",
    connection: MyApp.MyConnection
end
```

Then you can publish messages to the stream:

```elixir
MyApp.MyProducer.publish("Hello World")
```

## Consuming

First you define a connection

```elixir
defmodule MyApp.MyConnection do
  use RabbitMQStream.Connection
end
```

You then can declare a consumer module with the `RabbitMQStream.Consumer`:

```elixir
defmodule MyApp.MyConsumer do
  use RabbitMQStream.Consumer,
    connection: MyApp.MyConnection,
    stream_name: "my_stream",
    initial_offset: :first

  @impl true
  def handle_message(_message) do
    :ok
  end
end
```

Or you could manually consume from the stream with

```elixir
{:ok, _subscription_id} = MyApp.MyConnection.subscribe("stream-01", self(), :next, 999)
```

The caller process will start receiving messages with the format `{:deliver, %RabbitMQStream.Message.Types.DeliverData{} = deliver_data}`

```elixir
def handle_info({:deliver, %RabbitMQStream.Message.Types.DeliverData{} = deliver_data}, state) do
  # do something with message
  {:noreply, state}
end
```

## [SuperStreams](https://github.com/VictorGaiva/rabbitmq-stream/blob/main/guides/concepts/super-streams.md)

[A super stream is a logical stream made of individual, regular streams.](https://www.rabbitmq.com/blog/2022/07/13/rabbitmq-3-11-feature-preview-super-streams)

You can declare SuperStreams with:

```elixir
:ok = MyApp.MyConnection.create_super_stream("my_super_stream", "route-A": ["stream-01", "stream-02"], "route-B": ["stream-03"])
```

And you can consume from it with:

```elixir
defmodule MyApp.MySuperConsumer do
  use RabbitMQStream.SuperConsumer,
    initial_offset: :next,
    super_stream: "my_super_stream"

  @impl true
  def handle_message(_message) do
    # ...
    :ok
  end
end
```

### Configuration

The configuration for the connection can be set in your `config.exs` file:

```elixir
config :rabbitmq_stream, MyApp.MyConnection,
  username: "guest",
  password: "guest"
  # ...
end

```

You can configure a default Serializer module by passing it to the defaults configuration option

```elixir
config :rabbitmq_stream, :defaults,
  serializer: Jason
end
```

### TLS

You can configure the RabbitmqStream to use TLS connections:

```elixir
coonfig :rabbitmq_stream, :defaults,
  connection: [
    transport: :ssl,
    ssl_opts: [
      keyfile: "services/cert/client_box_key.pem",
      certfile: "services/cert/client_box_certificate.pem",
      cacertfile: "services/cert/ca_certificate.pem"
    ]
  ]
```

For more information, check the [documentation](https://hexdocs.pm/rabbitmq_stream/).
