# Overview

## Publishing messages

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

The module's name is used as the `reference_name` with the RabbitMQ server to avoid de-duplication.
