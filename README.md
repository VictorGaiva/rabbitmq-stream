# RabbitMQStream

[![Version](https://img.shields.io/hexpm/v/rabbitmq_stream.svg)](https://hex.pm/packages/rabbitmq_stream)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/rabbitmq_stream/)
[![Download](https://img.shields.io/hexpm/dt/rabbitmq_stream.svg)](https://hex.pm/packages/rabbitmq_stream)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Elixir Client for [RabbitMQ Streams Protocol](https://www.rabbitmq.com/streams.html).

## Usage

### Subscribing to stream

First you define a connection

```elixir
defmodule MyApp.MyConnection do
  use RabbitMQStream.Connection
end
```

Then you can subscribe to messages from a stream:

```elixir
{:ok, _subscription_id} = MyApp.MyConnection.subscribe("stream-01", self(), :next, 999)
```

### Publishing to stream

RabbitMQ Streams protocol needs a static `:reference_name` per publisher. This is used to prevent message duplication. For this reason, each stream needs, for now, a static module to publish messages, which keeps track of its own `publishing_id`.

You can define a `Publisher` module like this:

```elixir
defmodule MyApp.MyPublisher do
  use RabbitMQStream.Publisher,
    stream: "stream-01",
    connection: MyApp.MyConnection
end
```

Then you can publish messages to the stream:

```elixir
MyApp.MyPublisher.publish("Hello World")
```

## Installation

The package can be installed by adding `rabbitmq_stream` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:rabbitmq_stream, "~> 0.1.0"},
    # ...
  ]
end
```

For more information, check the [documentation](https://hexdocs.pm/rabbitmq_stream/).
