# Offset

When consuming from a Stream, you might be interested in consuming all the messages all the way from the beggining, the end, or somewhere in between. Wehn we start consuming from a stream, we tell RabbitMQ where we want to start consuming from, which is called the `offset`.

When starting to consume from a stream, we have some options for the offset:

<!-- :first | :last | :next | {:offset, non_neg_integer()} | {:timestamp, integer()} -->

* `:first` - Consume all the messages from the stream, starting from the beggining.
* `:last` - Consume the last message in the stream at the moment the consumer starts, and all the messages that are published after that.
* `:next` - Consume only the messages that are published after the consumer starts.
* `{:offset, non_neg_integer()}` - Consume all the messages from the stream, starting from the offset provided. Each chunk's offset is present in its metadata.
* `{:timestamp, integer()}` - Consume all the messages from the stream, starting from the message that was published at the timestamp provided.

## Example

When calling the `RabbitMQStream.Connection.subscribe/5` callback, we can provide the `:offset` option:

```elixir
defmodule MyApp.MyConnection
  use RabbitMQStream.Connection
end

{:ok, _} = MyApp.MyConnection.start_link()


{:ok, subcription_id} = RabbitMQStream.Connection.subscribe(
  "stream-name-01",
  self(),
  :first,
  50_000,
  []
)
```

Or you can provide the `:initial_offset` option to `RabbitMQStream.Consumer`:

```elixir
defmodule MyApp.MyConsumer do
  use RabbitMQStream.Consumer,
    connection: MyApp.MyConnection,
    stream_name: "my_stream",
    offset_reference: "default", # defaults to the module's name. E.g. MyApp.MyConsumer
    initial_offset: :first

  @impl true
  def handle_chunk(%RabbitMQStream.OsirisChunk{} = _chunk, _consumer) do
    :ok
  end
end
```

## Offset Tracking

Altough the RabbitMQ server doesn't automatically tracks the offset of each consumer, it provides a `store_offset` command. This allows us to store a piece of information, the `offset`, which is referenced by a `reference_name`, on the stream itself. We can then use it to retreive the offset later on, when starting the Consumer.

A `RabbitMQStream.Consumer` instance automatically queries the offset under `reference_name` on startup, and uses it as the offset passed to the subscribe command. It automatically stores the offset based on customizeable strategies.

By default it uses the `RabbitMQStream.Consumer.OffsetTracking.CountStrategy` strategy, storing the offset whenever `count` messages are received. It can be used with:

```elixir
alias RabbitMQStream.Consumer.OffsetTracking.CountStrategy

use RabbitMQStream.Consumer,
  stream_name: "my_stream",
  offset_tracking: [CountStrategy, store_after: 50]
  # ...

# The macro also accepts a simplified alias `count`
use RabbitMQStream.Consumer,
  stream_name: "my_stream",
  offset_tracking: [count: [store_after: 50]]

# You can also declare multiple strategies to run concurrently.
use RabbitMQStream.Consumer,
  stream_name: "my_stream",
  offset_tracking: [
    count: [store_after: 50],
    interval: [interval: 10_000] # RabbitMQStream.Consumer.OffsetTracking.IntervalStrategy
  ]
```

You can also implement you own strategy by implementing the `RabbitMQStream.Consumer.OffsetTracking` behavior, and passing it to the `offset_tracking` option.
