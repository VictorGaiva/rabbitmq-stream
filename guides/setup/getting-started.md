# Getting Started

## Installation

First add RabbitMQ to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:rabbitmq_stream, "~> 0.4.2"},
    # ...
  ]
end
```

## Consuming from stream

First you define a connection

```elixir
defmodule MyApp.MyConnection
  use RabbitMQStream.Connection
end
```

You can configure the connection in your `config.exs` file:

```elixir
config :rabbitmq_stream, MyApp.MyConnection,
  vhost: "/"
```

Manually starting the connection is as simple as:

```elixir
{:ok, _} = MyApp.MyConnection.start_link()
```

Then you can consume to messages from a stream with:

```elixir
{:ok, _subscription_id} = MyApp.MyConnection.subscribe("stream-01", self(), :next, 999)
```

Which will the start sending the caller process messages containg chunks from the stream, with the format `{:deliver, %RabbitMQStream.Message.Types.DeliverData{} = deliver_data}`, which you can handle with, for example, in your `handle_info/2` callback on your GenServer module:

```elixir
def handle_info({:deliver, %RabbitMQStream.Message.Types.DeliverData{} = deliver_data}, state) do
  # do something with message
  {:noreply, state}
end
```

You can also define a Consumer module that subscribes to a stream, and keeps track of its credits and offset.

```elixir
defmodule MyApp.MyConsumer do
  use RabbitMQStream.Consumer,
    connection: MyApp.MyConnection,
    stream_name: "my_stream",
    initial_offset: :first

  @impl true
  def handle_message(_message) do
    # ...
    :ok
  end
end
```

Just add it to your supervision tree, and it will start consuming from the stream.

```elixir
children = [
  MyApp.MyConnection,
  MyApp.MyConsumer
  # ...
]
```

## Publishing to stream

To prevent message duplication, RabbitMQ requires us to declare a named Producer before being able to publish messages to a stream. We can do this by `using` the `RabbitMQStream.Producer`, which declare itself to the Connection, with the specified `:reference_name`, defaulting to the module's name.

```elixir
defmodule MyApp.MyProducer
  use RabbitMQStream.Producer,
    stream_name: "my_stream",
    connection: MyApp.MyConnection
end
```

Then you can publish messages to the stream with:

```elixir
MyApp.MyProducer.publish("Hello World")
```
