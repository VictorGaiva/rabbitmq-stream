defmodule RabbitMQStream.Consumer.Behaviour do
  @doc """
  The callback that is invoked when a chunk is received.

  The server sends us messages in chunks, which of each contains many messages. But if we are
  consuming from an offset inside of a chunk, or if we have enabled the `filter_value` parameter,
  some of those messages might not be passed to `handle_message/1` callback.

  You can use use `handle_chunk/1` to access the whole chunk for some extra logic.

  Implemeting `handle_chunk/1` doesn't prevent `handle_message/1` from being called.

  Optionally if you implement `handle_chunk/2`, it also passes the current
  state of the consumer. It can be used to access the `private` field
  passed to `start_link/1`, or the `stream_name` itself.

  """
  @callback handle_chunk(chunk :: RabbitMQStream.OsirisChunk.t()) :: term()
  @callback handle_chunk(chunk :: RabbitMQStream.OsirisChunk.t(), state :: RabbitMQStream.Consumer.t()) :: term()

  @doc """
  Callback invoked on each message received from the stream.

  This is the main way of consuming messages, and it applies any filtering and decoding necessary
  to the message before being invoked.
  """
  @callback handle_message(message :: binary() | term()) :: term()
  @callback handle_message(message :: binary() | term(), state :: RabbitMQStream.Consumer.t()) :: term()
  @callback handle_message(
              message :: binary() | term(),
              chunk :: RabbitMQStream.OsirisChunk.t(),
              state :: RabbitMQStream.Consumer.t()
            ) ::
              term()

  @doc """
  If the consumer has been defined with the 'single-active-consumer' parameter,
  this callback is invoked when the consumer is being upgraded to being the
  active one, or when downgraded to being an inactive one.

  When the flag parameter is set to ':upgrade', it means that the consumer is being
  upgraded to active and it must return the offset for where it wants to start
  consuming from the stream.

  When being downgraded, the offset returned by the callback is also sent
  to the server but, at the moment, is not being used in any way, and is only
  sent because the API requires. But this is actually a good moment to store
  the offset so that it can be retrieved by the other consumer that is being
  upgraded.
  """
  @callback handle_update(consumer :: RabbitMQStream.Consumer.t(), action :: :upgrade | :downgrade) ::
              {:ok, RabbitMQStream.Connection.offset()} | {:error, any()}

  @doc """
  Callback invoked on each message inside of a chunk.

  It can be used to decode the message from a binary format into a Map,
  or to use GZIP to decompress the content.

  You can also globally define a 'Serializer' module, that must implement
  the 'decode!/1' callback, at compile-time configuration so it is added
  to as the default callback.
  """
  @callback decode!(message :: String.t()) :: term()

  @doc """
  Callback invoked right before subscribing a consumer to the stream.
  Might be usefull for setup logic, like creating a stream if it doesn't yet exists.
  """
  @callback before_start(RabbitMQStream.Consumer.opts(), RabbitMQStream.Consumer.t()) :: RabbitMQStream.Consumer.t()

  @doc """
  Send a command to add the provided amount of credits to the consumer.

  The credits are tracked by the Server, but it is also stored internally
  on the Consumer state, which then can be retreived by calling 'get_credits/0'.

  Always returns :ok, and any errors when adding credits to a consumer are logged.
  """
  @callback credit(amount :: non_neg_integer()) :: :ok

  @doc """
  Returns the internally tracked amount of credits for the Consumer.
  """
  @callback get_credits() :: non_neg_integer()

  @doc """
  Persists the consumer's latests offset into the stream.

  Be aware that it does not reset any tracking strategy.
  """
  @callback store_offset() :: :ok

  @doc """
  Computes the `filter_value` for a decoded messages, used for filtering incoming messages.

  Must follow the same implementation you define at your `c:RabbitMQStream.Producer.filter_value/1`
  callback.

  Required when passing either `:filter` or `:match_unfiltered` properties when declaring the consumer.

  Since the server sends the data in chunks, each of which is guaranted to have at least one message
  that match the consumer filter, but it might have some that don't. We need declare this callback to
  do additional client side filtering, so that the `handle_message/1` callback only receives those
  messages it is really interested in.
  """
  @callback filter_value(term()) :: binary() | nil

  @optional_callbacks handle_chunk: 1,
                      handle_chunk: 2,
                      handle_message: 1,
                      handle_message: 2,
                      handle_message: 3,
                      decode!: 1,
                      handle_update: 2,
                      before_start: 2,
                      get_credits: 0,
                      store_offset: 0,
                      filter_value: 1,
                      credit: 1
end
