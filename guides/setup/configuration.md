# Configuration

This library currently implements the following actors:

- `RabbitMQStream.Connection`: Manages a single TCP/SSL connection to a single RabbitMQ Stream node.
- `RabbitMQStream.Consumer`: Consumes from a single stream, while tracking its offset and credits. Requires an existing `RabbitMQStream.Connection`.
- `RabbitMQStream.Publisher`: Manages a single publisher to a single stream. Requires an existing `RabbitMQStream.Connection`.

## Configuration Merging

We can provide the configuration at many different levels, that will then be merged together. The order of precedence is:

### 1. Defaults

At your `config/*.exs` file, you can provide a default configuration that will be passed to all the instances of each of the actors. But some of these options are ignored as they don't really make sense to be set at the default level. For example, the `:stream_name` option is ignored by the `Publisher` actor. You can see which options are ignored in the documentation of each actor.

You can provide the default configuration like this:

```elixir
config :rabbitmq_stream, :defaults,
  connection: [
    vhost: "/",
    # RabbitMQStream.Connection.connection_option()
    # ...
  ],
  consumer: [
    connection: MyApp.MyConnection,
    # [RabbitMQStream.Consumer.consumer_option()]
    # ...
  ],
  publisher: [
    connection: MyApp.MyConnection,
    # [RabbitMQStream.Publisher.publisher_option()]
    # ...
  ]
```

### 2. Module specific configuration

Each time you implement an instance of the actor, with `use RabbitMQStream.Connection`, for example, you can define its configuration on your config file.

For example, if we have the following module:

```elixir
defmodule MyApp.MyConnection
  use RabbitMQStream.Connection
end
```

We can define its configuration like this:

```elixir
config :rabbitmq_stream, MyApp.MyConnection,
  vhost: "/"
```

### 3. When defining the actor

When defining the actor, you can provide the configuration as an option to the `use` macro:

```elixir
defmodule MyApp.MyConnection
  use RabbitMQStream.Connection,
    vhost: "/"
end
```

### 4. When starting the actor

When manually starting the actor, you can provide the configuration as an option to the `start_link` function:

```elixir
{:ok, _} = MyApp.MyConnection.start_link(vhost: "/")
```

Or pass the options when defining it in your supervision tree:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_, _) do
    children = [
      {MyApp.MyConnection, vhost: "/"},
      # ...
    ]

    opts = # ...
    Supervisor.start_link(children, opts)
  end
end
```

For more information, you can check the documentation at each actor's module;

## Global Options

### Serializer

You can define a Serializer module to be used by the Publisher and Consumer modules. It is expected to implement `encode!/1` and `decode!/1` callbacks, and must be defined at compile-time level configurations.
