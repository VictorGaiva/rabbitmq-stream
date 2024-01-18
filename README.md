# RabbitMQStream

[![Version](https://img.shields.io/hexpm/v/rabbitmq_stream.svg)](https://hex.pm/packages/rabbitmq_stream)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/rabbitmq_stream/)
[![Download](https://img.shields.io/hexpm/dt/rabbitmq_stream.svg)](https://hex.pm/packages/rabbitmq_stream)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Unit Tests](https://github.com/VictorGaiva/rabbitmq-stream/actions/workflows/ci.yaml/badge.svg)](https://github.com/VictorGaiva/rabbitmq-stream/actions)

Zero dependencies Elixir Client for [RabbitMQ Streams Protocol](https://www.rabbitmq.com/streams.html).

## Usage

### Consuming from stream

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
  def handle_chunk(%RabbitMQStream.OsirisChunk{}=_chunk, _consumer) do
    :ok
  end
end
```

Or you could manually consume from the stream with

```elixir
{:ok, _subscription_id} = MyApp.MyConnection.subscribe("stream-01", self(), :next, 999)
```

The caller process will start receiving messages with the format `{:chunk, %RabbitMQStream.OsirisChunk{} = chunk}`

```elixir
def handle_info({:chunk, %RabbitMQStream.OsirisChunk{} = chunk}, state) do
  # do something with message
  {:noreply, state}
end
```

You can take a look at an example Consumer GenServer at the [Consuming Documentation](guides/tutorial/consuming.md).

### Publishing to stream

RabbitMQ Streams protocol needs a static `:reference_name` per producer. This is used to prevent message duplication. For this reason, each stream needs, for now, a static module to publish messages, which keeps track of its own `publishing_id`.

You can define a `Producer` module like this:

```elixir
defmodule MyApp.MyProducer do
  use RabbitMQStream.Producer,
    stream: "stream-01",
    connection: MyApp.MyConnection
end
```

Then you can publish messages to the stream:

```elixir
MyApp.MyProducer.publish("Hello World")
```

## Installation

The package can be installed by adding `rabbitmq_stream` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:rabbitmq_stream, "~> 0.3.0"},
    # ...
  ]
end
```

## Configuration

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

## TLS Support

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
