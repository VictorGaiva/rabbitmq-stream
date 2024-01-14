defmodule RabbitMQStream.Connection do
  @moduledoc """
  Responsible for encoding and decoding messages, opening and maintaining a socket connection to a single node.
  It connects to the RabbitMQ, and authenticates, and mantains the connection open with heartbeats.

  # Adding a connectiong to the supervision tree

  You can define a connection with:

      defmodule MyApp.MyConnection
        use RabbitMQStream.Connection
      end


  Then you can add it to your supervision tree:

      def start(_, _) do
        children = [
          {MyApp.MyConnection, username: "guest", password: "guest", host: "localhost", vhost: "/"},
          # ...
        ]

        opts = # ...
        Supervisor.start_link(children, opts)
      end


  # Connection configuration
  The connection accept the following options:

  * `username` - The username to use for authentication. Defaults to `guest`.
  * `password` - The password to use for authentication. Defaults to `guest`.
  * `host` - The host to connect to. Defaults to `localhost`.
  * `port` - The port to connect to. Defaults to `5552`.
  * `vhost` - The virtual host to use. Defaults to `/`.
  * `frame_max` - The maximum frame size in Bytes. Defaults to `1_048_576`.
  * `heartbeat` - The heartbeat interval in seconds. Defaults to `60`.
  * `lazy` - If `true`, the connection won't starting until explicitly calling `connect/1`. Defaults to `false`.


  # Consuming messages
  You can consume messages by calling `subscribe/5`:

      {:ok, _subscription_id} = MyApp.MyConnection.subscribe("stream-01", self(), :next, 999)



  # Configuration
  The configuration for the connection can be set in your `config.exs` file:

      config :rabbitmq_stream, MyApp.MyConnection,
        username: "guest",
        password: "guest"
        # ...

  You can override each configuration option by manually passing each configuration on the `use` macro:

      defmodule MyApp.MyConnection
        use RabbitMQStream.Connection, username: "guest", password: "guest"
      end

  or when adding to the supervision tree:

      def start(_, _) do
        children = [
          {MyApp.MyConnection, username: "guest", password: "guest"},
          # ...
        ]

        opts = # ...
        Supervisor.start_link(children, opts)
      end

  The precedence order is is the same order as the examples above, from top to bottom.


  # Buffering

  Any call or cast to the connection while it is not connected will be buffered and executed once the connection is open.
  """

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts], location: :keep do
      import RabbitMQStream.Connection.Helpers

      @behaviour RabbitMQStream.Connection
      @opts opts

      def start_link(opts \\ []) when is_list(opts) do
        options =
          Application.get_env(:rabbitmq_stream, :defaults, [])
          |> Keyword.get(:connection, [])
          |> Keyword.merge(Application.get_env(:rabbitmq_stream, __MODULE__, []))
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)

        options =
          options
          |> Keyword.put_new(:host, "localhost")
          |> Keyword.put_new(:port, 5552)
          |> Keyword.put_new(:vhost, "/")
          |> Keyword.put_new(:username, "guest")
          |> Keyword.put_new(:password, "guest")
          |> Keyword.put_new(:frame_max, 1_048_576)
          |> Keyword.put_new(:heartbeat, 60)
          |> Keyword.put_new(:transport, :tcp)

        GenServer.start_link(RabbitMQStream.Connection.Lifecycle, options, name: __MODULE__)
      end

      def child_spec(opts) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
      end

      def stop(reason \\ :normal, timeout \\ :infinity) do
        GenServer.stop(__MODULE__, reason, timeout)
      end

      def connect() do
        GenServer.call(__MODULE__, {:connect})
      end

      def close(reason \\ "", code \\ 0x00) do
        GenServer.call(__MODULE__, {:close, reason, code})
      end

      def create_stream(name, arguments \\ []) when is_binary(name) do
        GenServer.call(__MODULE__, {:create_stream, [name: name, arguments: arguments]})
      end

      def delete_stream(name) when is_binary(name) do
        GenServer.call(__MODULE__, {:delete_stream, name: name})
      end

      def store_offset(stream_name, offset_reference, offset)
          when is_binary(stream_name) and
                 is_binary(offset_reference) and
                 is_integer(offset) do
        GenServer.cast(
          __MODULE__,
          {:store_offset, stream_name: stream_name, offset_reference: offset_reference, offset: offset}
        )
      end

      def query_offset(stream_name, offset_reference)
          when is_binary(offset_reference) and
                 is_binary(stream_name) do
        GenServer.call(__MODULE__, {:query_offset, stream_name: stream_name, offset_reference: offset_reference})
      end

      def declare_publisher(stream_name, publisher_reference)
          when is_binary(publisher_reference) and
                 is_binary(stream_name) do
        GenServer.call(
          __MODULE__,
          {:declare_publisher, stream_name: stream_name, publisher_reference: publisher_reference}
        )
      end

      def delete_publisher(publisher_id)
          when is_integer(publisher_id) and
                 publisher_id <= 255 do
        GenServer.call(__MODULE__, {:delete_publisher, publisher_id: publisher_id})
      end

      def query_metadata(streams)
          when is_list(streams) and
                 length(streams) > 0 do
        GenServer.call(__MODULE__, {:query_metadata, streams: streams})
      end

      def query_publisher_sequence(stream_name, publisher_reference)
          when is_binary(publisher_reference) and
                 is_binary(stream_name) do
        GenServer.call(
          __MODULE__,
          {:query_publisher_sequence, publisher_reference: publisher_reference, stream_name: stream_name}
        )
      end

      def publish(publisher_id, publishing_id, message, filter_value \\ nil)
          when is_integer(publisher_id) and
                 is_binary(message) and
                 is_integer(publishing_id) and
                 (filter_value == nil or is_binary(filter_value)) and
                 publisher_id <= 255 do
        GenServer.cast(
          __MODULE__,
          {:publish, publisher_id: publisher_id, messages: [{publishing_id, message, filter_value}]}
        )
      end

      def subscribe(stream_name, pid, offset, credit, properties \\ [])
          when is_binary(stream_name) and
                 is_integer(credit) and
                 is_offset(offset) and
                 is_list(properties) and
                 is_pid(pid) and
                 credit >= 0 do
        GenServer.call(
          __MODULE__,
          {:subscribe, stream_name: stream_name, pid: pid, offset: offset, credit: credit, properties: properties}
        )
      end

      def unsubscribe(subscription_id) when subscription_id <= 255 do
        GenServer.call(__MODULE__, {:unsubscribe, subscription_id: subscription_id})
      end

      def credit(subscription_id, credit) when is_integer(subscription_id) and credit >= 0 do
        GenServer.cast(__MODULE__, {:credit, subscription_id: subscription_id, credit: credit})
      end

      def route(routing_key, super_stream) when is_binary(routing_key) and is_binary(super_stream) do
        GenServer.call(__MODULE__, {:route, routing_key: routing_key, super_stream: super_stream})
      end

      def stream_stats(stream_name) when is_binary(stream_name) do
        GenServer.call(__MODULE__, {:stream_stats, stream_name: stream_name})
      end

      def partitions(super_stream) when is_binary(super_stream) do
        GenServer.call(__MODULE__, {:partitions, super_stream: super_stream})
      end

      def create_super_stream(name, partitions, binding_keys, arguments \\ [])
          when is_binary(name) and
                 is_list(partitions) and
                 length(partitions) > 0 and
                 is_list(binding_keys) and
                 length(binding_keys) > 0 do
        GenServer.call(
          __MODULE__,
          {:create_super_stream, name: name, partitions: partitions, binding_keys: binding_keys, arguments: arguments}
        )
      end

      def delete_super_stream(name) when is_binary(name) do
        GenServer.call(__MODULE__, {:delete_super_stream, name: name})
      end

      def respond(request, opts) when is_list(opts) do
        GenServer.cast(__MODULE__, {:respond, request, opts})
      end
    end
  end

  @type offset :: :first | :last | :next | {:offset, non_neg_integer()} | {:timestamp, integer()}
  @type connection_options :: [connection_option]
  @type connection_option ::
          {:username, String.t()}
          | {:password, String.t()}
          | {:host, String.t()}
          | {:port, non_neg_integer()}
          | {:vhost, String.t()}
          | {:frame_max, non_neg_integer()}
          | {:heartbeat, non_neg_integer()}
          | {:lazy, boolean()}

  @callback start_link([connection_option | {:name, atom()}]) :: :ignore | {:error, any} | {:ok, pid}

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
              offset :: offset(),
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

  @type t() :: %RabbitMQStream.Connection{
          options: connection_options,
          socket: :gen_tcp.socket(),
          state: :closed | :connecting | :open | :closing,
          correlation_sequence: non_neg_integer(),
          publisher_sequence: non_neg_integer(),
          subscriber_sequence: non_neg_integer(),
          peer_properties: [[String.t()]],
          connection_properties: %{String.t() => String.t() | integer()},
          mechanisms: [String.t()],
          connect_requests: [pid()],
          request_tracker: %{{atom(), integer()} => {pid(), any()}},
          brokers: %{integer() => RabbitMQStream.Message.Types.QueryPublisherSequenceData.t()},
          streams: %{String.t() => RabbitMQStream.Message.Types.QueryPublisherSequenceData.t()},
          subscriptions: %{non_neg_integer() => pid()},
          server_commands_versions: %{
            RabbitMQStream.Message.Helpers.command() => {non_neg_integer(), non_neg_integer()}
          },
          frames_buffer: RabbitMQStream.Message.Buffer.t(),
          request_buffer: :queue.queue({term(), pid()}),
          commands_buffer: :queue.queue({atom(), atom(), list({atom(), term()})}),
          # this should not be here. Should find a better way to return the close reason from the 'handler' module
          close_reason: String.t() | atom() | nil,
          transport: RabbitMQStream.Connection.Transport.t()
        }
  @enforce_keys [:transport, :options]
  defstruct [
    :socket,
    :transport,
    options: [],
    correlation_sequence: 1,
    publisher_sequence: 1,
    subscriber_sequence: 1,
    subscriptions: %{},
    state: :closed,
    peer_properties: [],
    connection_properties: [],
    mechanisms: [],
    connect_requests: [],
    request_tracker: %{},
    brokers: %{},
    streams: %{},
    server_commands_versions: [],
    request_buffer: :queue.new(),
    frames_buffer: RabbitMQStream.Message.Buffer.init(),
    commands_buffer: :queue.new(),
    close_reason: nil
  ]
end
