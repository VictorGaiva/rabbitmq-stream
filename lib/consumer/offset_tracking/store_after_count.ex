defmodule RabbitMQStream.Consumer.OffsetTracking.CountStrategy do
  @behaviour RabbitMQStream.Consumer.OffsetTracking

  @moduledoc """
  Count Strategy

  Stores the offset after every `store_after` messages.

  # Usage
      defmodule MyApp.MyConsumer do
        alias RabbitMQStream.Consumer.OffsetTracking

        use RabbitMQStream.Consumer,
          offset_tracking: [OffsetTracking.CountStrategy, store_after: 50]

        @impl true
        def handle_chunk(_chunk, _subscriber) do
          :ok
        end
      end


  # Parameters

  * `store_after` - the number of messages to receive before storing the offset

  """

  @doc false
  def init(opts \\ []) do
    store_after = Keyword.get(opts, :store_after, 50)
    {0, store_after}
  end

  @doc false
  def after_chunk(
        {count, store_after},
        %RabbitMQStream.OsirisChunk{num_entries: num_entries},
        _
      ) do
    {count + num_entries, store_after}
  end

  def after_chunk(state, _, _), do: state

  @doc false
  def run({count, store_after}, _) when count >= store_after, do: {:store, {0, store_after}}
  def run(state, _), do: {:skip, state}
end
