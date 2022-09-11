# Getting Started

## Installation

First add RabbitMQ to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:rabbitmq_stream, "~> 0.1.0"},
    # ...
  ]
end
```

After running `mix deps.get`, you add a `RabbitMQStream.Connection` to your supervision tree with:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      {RabbitMQStream.Connection, username: "guest", password: "guest", host: "localhost", vhost: "/"}},
      # ...
    ]

    opts = # ...
    Supervisor.start_link(children, opts)
  end
end
```

## Publishing messages

You can create a standalone Publisher for a static stream.

```elixir
defmodule MyApp.MyPublisher do
  use RabbitMQStream.Publisher,
    stream_name: "my-stream"
end
```

You can now publish messages to the stream with

```elixir
MyApp.MyPublisher.publish("Hello, world!")
```

## Subscribing
