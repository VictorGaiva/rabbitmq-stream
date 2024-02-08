# Producing

The RabbitMQ server expects us to declare a named producer, before being able to start publishing messages. You can declare a Producer Module as the following:

```elixir
defmodule MyApp.MyProducer do
  use RabbitMQStream.Producer,
    stream_name: "my-stream"
end
```

You can publish a message to the stream by calling its `publish/1` callback with the binary contents of the message:
  
```elixir
MyApp.MyProducer.publish("my-message")
```

## Message De-duplication

By default it uses the module's name as the `:reference_name` passed to RabbitMQ. Internally the producer module keeps track of a `publishing_id` sequence, which is incremented after every message.

These two parameters are used by the RabbitMQ server to prevent message duplication. If it receives two messages from the same `reference_name` with the same `publishing_id`, it simply drops the message.

## Filter Value

As of RabbitMQ 3.13, you can provide a `filter_value` for each message, that are internally persisted alongside each message, and can be used to segment the consumption of messages. The filter values can then be provided when declaring a Consumer, so the server only sends chunks which that [might](https://blog.rabbitmq.com/posts/2023/10/stream-filtering/#on-the-consumer-side) contain messages which the consumer is interested in.

This might be useful when you that you will be processing only subsets of the messages of each stream, at a time, will already be doing some filtering at the client side, as it saves bandwidth by sending only the data

When declaring a Producer/SuperProducer, you can optionally declare a `filter_value/1` callback that must return a `binary()` value that will be sent as the message's `filter_value`.

```elixir
defmodule MyApp.MyProducer do
  use RabbitMQStream.Producer,
    stream_name: "my-stream"

  @impl true
  def filter_value(message) do
    message["region"]
  end
end
```

Then when consuming you can provide a `:filter` property to the Consumer/SuperConsumer as the following:

```elixir
defmodule MyApp.MyConsumer do
  use RabbitMQStream.Consumer,
    initial_offset: :next,
    stream_name: "my-stream",
    properties: [
      filter: ["latam", "eu"]
    ]

  @impl true
  def handle_chunk(chunk, _) do
    for message <- chunk.data_entries do
      dbg(message)
    end

    :ok
  end
end
```
