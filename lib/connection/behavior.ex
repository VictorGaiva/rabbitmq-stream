defmodule RabbitMQStream.Connection.Behavior do
  @callback start_link([RabbitMQStream.Connection.connection_option() | {:name, atom()}]) ::
              :ignore | {:error, any} | {:ok, pid}

  @callback connect() :: :ok | {:error, reason :: atom()}

  @callback close(reason :: String.t(), code :: integer()) ::
              :ok | {:error, reason :: atom()}

  @callback create_stream(String.t(), keyword(String.t())) ::
              :ok | {:error, reason :: atom()}

  @callback delete_stream(String.t()) :: :ok | {:error, reason :: atom()}

  @callback store_offset(String.t(), String.t(), integer()) :: :ok

  @callback query_offset(String.t(), String.t()) ::
              {:ok, offset :: integer()} | {:error, reason :: atom()}

  @callback declare_publisher(String.t(), String.t()) ::
              {:ok, publisher_id :: integer()} | {:error, any()}

  @callback delete_publisher(publisher_id :: integer()) ::
              :ok | {:error, reason :: atom()}

  @callback query_metadata([String.t(), ...]) ::
              {:ok, metadata :: %{brokers: any(), streams: any()}}
              | {:error, reason :: atom()}

  @callback query_publisher_sequence(String.t(), String.t()) ::
              {:ok, sequence :: integer()} | {:error, reason :: atom()}

  @callback publish(integer(), integer(), binary()) :: :ok

  @callback subscribe(
              stream_name :: String.t(),
              pid :: pid(),
              offset :: RabbitMQStream.Connection.offset(),
              credit :: non_neg_integer(),
              properties :: %{String.t() => String.t()}
            ) :: {:ok, subscription_id :: non_neg_integer()} | {:error, reason :: atom()}

  @callback unsubscribe(subscription_id :: non_neg_integer()) ::
              :ok | {:error, reason :: atom()}

  @doc """
  The server will sometimes send a request to the client, which we must send a response to.

  And example request is the 'ConsumerUpdate', where the server expects a response with the
  offset. So the connection sends the request to the subscription handler, which then calls
  this function to send the response back to the server.
  """
  @callback respond(request :: RabbitMQStream.Message.Request.t(), opts :: Keyword.t()) :: :ok

  @doc """
  Adds the specified amount of credits to the subscription under the given `subscription_id`.

  This function instantly returns `:ok` as the RabbitMQ Server only sends a response if the command fails,
  which only happens if the subscription is not found. In that case the error is logged.

  """
  @callback credit(subscription_id :: non_neg_integer(), credit :: non_neg_integer()) :: :ok
end
