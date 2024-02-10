# AMPQ Interop

When using RabbitMQ, you are able to setup a queue as a Stream, and its messages can be consumed by any AMQP Client, or using the Streams Protocol. Using the [Elixir AMQP Client](https://github.com/pma/amqp), you can declare a hibrid queue as follows:

```elixir
{:ok, conn} = AMQP.Connection.open()
{:ok, channel} = AMQP.Channel.open(conn)

{:ok, _} =
  AMQP.Queue.declare(
    channel,
    "my-stream",
    durable: true,
    arguments: [{"x-queue-type", "stream"}]
  )

:ok = AMQP.Exchange.declare(channel, "my-exchange")
AMQP.Queue.bind(channel, "my-stream", "my-exchange")

:ok = AMQP.Basic.publish(channel, "my-exchange", "", "Hello, World!")

```

Then any messages published to the queue are persited with `amqp1.0` binary format. You are able to decode the content of the message using the `:amqp10_framing.decode_bin/1` function of the [`:amqp_common`](https://hex.pm/packages/amqp10_common) library.

You can configure a Consumer Module to get only the content itself of each message by declaring the `decode!/1` callback.

```elixir
defmodule MyApp.MyConsumer do
  use RabbitMQStream.Consumer,
    initial_offset: :next,
    stream_name: "my-stream"
  
  @impl true
  def decode!(message) do
    :amqp10_framing.decode_bin(message)[:"v1_0.data"]
  end

  @impl true
  def handle_chunk(%RabbitMQStream.OsirisChunk{}=chunk, _) do
    for message <- chunk.data_entries do
      dbg(message)
    end

    :ok
  end
end
```

You can get more information on interop concepts on the related [Interoperability in RabbitMQ Streams](https://blog.rabbitmq.com/posts/2021/10/rabbitmq-streams-interoperability/) post.
