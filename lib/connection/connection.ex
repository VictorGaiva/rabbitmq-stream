defmodule RabbitMQStream.Connection do
  @moduledoc """
  Responsible for encoding and decoding messages, opening and maintaining a socket connection to a single node.
  It connects to the RabbitMQ, and authenticates, and mantains the connection open with heartbeats.

  ## Adding a connectiong to the supervision tree

      def start(_, _) do
        children = [
          {RabbitMQStream.Connection, username: "guest", password: "guest", host: "localhost", vhost: "/"}},
          # ...
        ]

        opts = # ...
        Supervisor.start_link(children, opts)
      end


  ## Connection configuration
  The connection accept the following options:

  - `username` - The username to use for authentication. Defaults to `guest`.
  - `password` - The password to use for authentication. Defaults to `guest`.
  - `host` - The host to connect to. Defaults to `localhost`.
  - `port` - The port to connect to. Defaults to `5552`.
  - `vhost` - The virtual host to use. Defaults to `/`.
  - `frame_max` - The maximum frame size in Bytes. Defaults to `1_048_576`.
  - `heartbeat` - The heartbeat interval in seconds. Defaults to `60`.
  - `lazy` - If `true`, the connection won't starting until explicitly calling `connect/1`. Defaults to `false`.

  """

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

  @type t() :: %RabbitMQStream.Connection{
          options: connection_options,
          socket: :gen_tcp.socket(),
          version: 1,
          state: :closed | :connecting | :open | :closing,
          correlation_sequence: integer(),
          publisher_sequence: non_neg_integer(),
          subscriber_sequence: non_neg_integer(),
          peer_properties: [[String.t()]],
          connection_properties: %{String.t() => String.t() | integer()},
          mechanisms: [String.t()],
          connect_requests: [pid()],
          request_tracker: %{{atom(), integer()} => {pid(), any()}},
          brokers: %{integer() => BrokerData.t()},
          streams: %{String.t() => StreamData.t()},
          subscriptions: %{non_neg_integer() => pid()},
          buffer: :rabbit_stream_core.state()
        }

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

  defstruct [
    :socket,
    options: [],
    version: 1,
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
    buffer: :rabbit_stream_core.init("")
  ]

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use RabbitMQStream.Connection.Lifecycle
      @behaviour RabbitMQStream.Connection
      @opts opts

      def start_link(args \\ []) when is_list(args) do
        args = Keyword.merge(@opts, args)
        GenServer.start_link(__MODULE__, args, name: __MODULE__)
      end

      @impl true
      def init(opts) do
        conn = %RabbitMQStream.Connection{
          options: [
            host: opts[:host] || "localhost",
            port: opts[:port] || 5552,
            vhost: opts[:vhost] || "/",
            username: opts[:username] || "guest",
            password: opts[:password] || "guest",
            frame_max: opts[:frame_max] || 1_048_576,
            heartbeat: opts[:heartbeat] || 60
          ]
        }

        if opts[:lazy] == true do
          {:ok, conn}
        else
          {:ok, conn, {:continue, {:connect}}}
        end
      end

      def connect() do
        GenServer.call(__MODULE__, {:connect})
      end

      def close(reason \\ "", code \\ 0x00) do
        GenServer.call(__MODULE__, {:close, reason, code})
      end

      def create_stream(name, arguments \\ []) when is_binary(name) do
        GenServer.call(__MODULE__, {:create_stream, arguments ++ [name: name]})
      end

      def delete_stream(name) when is_binary(name) do
        GenServer.call(__MODULE__, {:delete_stream, name: name})
      end

      def store_offset(stream_name, offset_reference, offset)
          when is_binary(stream_name) and
                 is_binary(offset_reference) and
                 is_integer(offset) do
        GenServer.call(
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

      def publish(publisher_id, publishing_id, message)
          when is_integer(publisher_id) and
                 is_binary(message) and
                 is_integer(publishing_id) and
                 publisher_id <= 255 do
        GenServer.cast(
          __MODULE__,
          {:publish, publisher_id: publisher_id, published_messages: [{publishing_id, message}], wait: true}
        )
      end

      def subscribe(stream_name, pid, offset, credit, properties \\ %{})
          when ((is_binary(stream_name) and
                   is_integer(credit) and
                   offset in [:first, :last, :next]) or
                  (tuple_size(offset) == 2 and is_tuple(offset) and elem(offset, 0) in [:offset, :timestamp])) and
                 is_map(properties) and
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

      if Mix.env() == :test do
        def get_state() do
          GenServer.call(__MODULE__, {:get_state})
        end
      end
    end
  end
end
