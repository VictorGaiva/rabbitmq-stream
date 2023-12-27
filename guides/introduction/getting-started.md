# Getting Started

## Installation

First add RabbitMQ to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:rabbitmq_stream, "~> 0.2.1"},
    # ...
  ]
end
```

## Subscribing to stream

First you define a connection

```elixir
defmodule MyApp.MyConnection
  use RabbitMQStream.Connection
end
```

Then you can subscribe to messages from a stream:

```elixir
{:ok, _subscription_id} = MyApp.MyConnection.subscribe("stream-01", self(), :next, 999)
```

The caller process will start receiving messages with the format `{:message, RabbitMQStream.Message.Data.DeliverData}`

```elixir
def handle_info({:message, RabbitMQStream.Message.Data.DeliverData = message}, state) do
  # do something with message
  {:noreply, state}
end
```

## Publishing to stream

RabbitMQ Streams protocol needs a static `:reference_name` per publisher. This is used to prevent message duplication. For this reason, each stream needs, for now, a static module to publish messages, which keeps track of its own `publishing_id`.

You can define a `Publisher` module like this:

```elixir
defmodule MyApp.MyPublisher
  use RabbitMQStream.Publisher,
    stream: "stream-01",
    connection: MyApp.MyConnection
end
```

Then you can publish messages to the stream:

```elixir
MyApp.MyPublisher.publish("Hello World")
```
