defmodule RabbitMQStream.Connection.Behavior do
  @callback start_link([RabbitMQStream.Connection.connection_option() | {:name, atom()}]) ::
              :ignore | {:error, any} | {:ok, pid}

  @doc """
  Starts the connection process with the RabbitMQ Stream server, and waits
  until the authentication is complete.

  Waits for the connection process if it is already started, and instantly
  returns if the connection is already established.

  """
  @callback connect(GenServer.server()) :: :ok | {:error, reason :: atom()}

  @callback close(GenServer.server(), reason :: String.t(), code :: integer()) ::
              :ok | {:error, reason :: atom()}

  @callback create_stream(GenServer.server(), String.t(), keyword(String.t())) ::
              :ok | {:error, reason :: atom()}

  @callback delete_stream(GenServer.server(), String.t()) :: :ok | {:error, reason :: atom()}

  @callback store_offset(GenServer.server(), String.t(), String.t(), integer()) :: :ok

  @callback query_offset(GenServer.server(), String.t(), String.t()) ::
              {:ok, offset :: integer()} | {:error, reason :: atom()}

  @callback declare_producer(GenServer.server(), String.t(), String.t()) ::
              {:ok, producer_id :: integer()} | {:error, any()}

  @callback delete_producer(GenServer.server(), producer_id :: integer()) ::
              :ok | {:error, reason :: atom()}

  @callback query_metadata(GenServer.server(), [String.t(), ...]) ::
              {:ok, metadata :: %{brokers: any(), streams: any()}}
              | {:error, reason :: atom()}

  @callback query_producer_sequence(GenServer.server(), String.t(), String.t()) ::
              {:ok, sequence :: integer()} | {:error, reason :: atom()}

  @callback publish(GenServer.server(), integer(), integer(), binary()) :: :ok

  @callback subscribe(
              GenServer.server(),
              stream_name :: String.t(),
              pid :: pid(),
              offset :: RabbitMQStream.Connection.offset(),
              credit :: non_neg_integer(),
              properties :: Keyword.t()
            ) :: {:ok, subscription_id :: non_neg_integer()} | {:error, reason :: atom()}

  @callback unsubscribe(GenServer.server(), subscription_id :: non_neg_integer()) ::
              :ok | {:error, reason :: atom()}

  @doc """
  The server will sometimes send a request to the client, which we must send a response to.

  And example request is the 'ConsumerUpdate', where the server expects a response with the
  offset. So the connection sends the request to the subscription handler, which then calls
  this function to send the response back to the server.
  """
  @callback respond(GenServer.server(), request :: RabbitMQStream.Message.Request.t(), opts :: Keyword.t()) :: :ok

  @doc """
  Adds the specified amount of credits to the subscription under the given `subscription_id`.

  This function instantly returns `:ok` as the RabbitMQ Server only sends a response if the command fails,
  which only happens if the subscription is not found. In that case the error is logged.

  """
  @callback credit(GenServer.server(), subscription_id :: non_neg_integer(), credit :: non_neg_integer()) :: :ok
end
