# Subscribing to Messages

After defining a connection with:

```elixir
defmodule MyApp.MyConnection do
  use RabbitMQStream.Connection
end
```

You can subscribe to messages from a stream with:

```elixir
{:ok, _subscription_id} = MyApp.MyConnection.subscribe("stream-01", self(), :next, 999)
```

The message will be received with the format `{:message, %RabbitMQStream.OsirisChunk{}}`.

## Example

An example `GenServer` handler that receives messages from a stream could be written like this:

```elixir
defmodule MyApp.MySubscriber do
  use GenServer
  alias RabbitMQStream.OsirisChunk

  def start_link do
    GenServer.start_link(__MODULE__, %{})
  end

  def init(state) do
    {:ok, subscription_id} = MyApp.MyConnection.subscribe("stream-01", self(), :next, 999)

    {:ok, Map.put(state, :subscription_id, subscription_id)}
  end

  def handle_info({:message, %OsirisChunk{} = message}, state) do
    # do something with message
    {:noreply, state}
  end
end
```
