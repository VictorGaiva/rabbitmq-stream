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
      @behaviour RabbitMQStream.Connection.Behavior
      @opts opts

      def start_link(opts \\ []) when is_list(opts) do
        opts =
          Application.get_env(:rabbitmq_stream, __MODULE__, [])
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)
          |> Keyword.put(:name, __MODULE__)

        RabbitMQStream.Connection.start_link(opts)
      end

      def child_spec(opts) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
      end

      def stop(reason \\ :normal, timeout \\ :infinity) do
        GenServer.stop(__MODULE__, reason, timeout)
      end

      def connect() do
        RabbitMQStream.Connection.connect(__MODULE__)
      end

      def close(reason \\ "", code \\ 0x00) do
        RabbitMQStream.Connection.close(__MODULE__, reason, code)
      end

      def create_stream(stream_name, arguments \\ []) do
        RabbitMQStream.Connection.create_stream(__MODULE__, stream_name, arguments)
      end

      def delete_stream(stream_name) do
        RabbitMQStream.Connection.delete_stream(__MODULE__, stream_name)
      end

      def store_offset(stream_name, offset_reference, offset) do
        RabbitMQStream.Connection.store_offset(__MODULE__, stream_name, offset_reference, offset)
      end

      def query_offset(stream_name, offset_reference) do
        RabbitMQStream.Connection.query_offset(__MODULE__, stream_name, offset_reference)
      end

      def declare_publisher(stream_name, publisher_reference) do
        RabbitMQStream.Connection.declare_publisher(
          __MODULE__,
          stream_name,
          publisher_reference
        )
      end

      def delete_publisher(publisher_id) do
        RabbitMQStream.Connection.delete_publisher(__MODULE__, publisher_id)
      end

      def query_metadata(streams) do
        RabbitMQStream.Connection.query_metadata(__MODULE__, streams)
      end

      def query_publisher_sequence(stream_name, publisher_reference) do
        RabbitMQStream.Connection.query_publisher_sequence(
          __MODULE__,
          publisher_reference,
          stream_name
        )
      end

      def publish(publisher_id, publishing_id, message, filter_value \\ nil) do
        RabbitMQStream.Connection.publish(
          __MODULE__,
          publisher_id,
          publishing_id,
          message,
          filter_value
        )
      end

      def subscribe(stream_name, pid, offset, credit, properties \\ []) do
        RabbitMQStream.Connection.subscribe(
          __MODULE__,
          stream_name,
          pid,
          offset,
          credit,
          properties
        )
      end

      def unsubscribe(subscription_id) do
        RabbitMQStream.Connection.unsubscribe(__MODULE__, subscription_id)
      end

      def credit(subscription_id, credit) do
        RabbitMQStream.Connection.credit(__MODULE__, subscription_id, credit)
      end

      def route(routing_key, super_stream) do
        RabbitMQStream.Connection.route(__MODULE__, routing_key, super_stream)
      end

      def stream_stats(stream_name) do
        RabbitMQStream.Connection.stream_stats(__MODULE__, stream_name)
      end

      def partitions(super_stream) do
        RabbitMQStream.Connection.partitions(__MODULE__, super_stream)
      end

      def create_super_stream(name, partitions, binding_keys, arguments \\ []) do
        RabbitMQStream.Connection.create_super_stream(
          __MODULE__,
          name,
          partitions,
          binding_keys,
          arguments
        )
      end

      def delete_super_stream(name) do
        RabbitMQStream.Connection.delete_super_stream(__MODULE__, name)
      end

      def respond(request, opts) do
        RabbitMQStream.Connection.respond(__MODULE__, request, opts)
      end
    end
  end

  import RabbitMQStream.Connection.Helpers

  def start_link(opts \\ []) when is_list(opts) do
    opts =
      Application.get_env(:rabbitmq_stream, :defaults, [])
      |> Keyword.get(:connection, [])
      |> Keyword.merge(opts)

    GenServer.start_link(RabbitMQStream.Connection.Lifecycle, opts, name: opts[:name])
  end

  def connect(server) do
    GenServer.call(server, {:connect})
  end

  def close(server, reason \\ "", code \\ 0x00) do
    GenServer.call(server, {:close, reason, code})
  end

  def create_stream(server, name, arguments \\ []) when is_binary(name) do
    GenServer.call(server, {:create_stream, [name: name, arguments: arguments]})
  end

  def delete_stream(server, name) when is_binary(name) do
    GenServer.call(server, {:delete_stream, name: name})
  end

  def store_offset(server, stream_name, offset_reference, offset)
      when is_binary(stream_name) and
             is_binary(offset_reference) and
             is_integer(offset) do
    GenServer.cast(
      server,
      {:store_offset, stream_name: stream_name, offset_reference: offset_reference, offset: offset}
    )
  end

  def query_offset(server, stream_name, offset_reference)
      when is_binary(offset_reference) and
             is_binary(stream_name) do
    GenServer.call(server, {:query_offset, stream_name: stream_name, offset_reference: offset_reference})
  end

  def declare_publisher(server, stream_name, publisher_reference)
      when is_binary(publisher_reference) and
             is_binary(stream_name) do
    GenServer.call(
      server,
      {:declare_publisher, stream_name: stream_name, publisher_reference: publisher_reference}
    )
  end

  def delete_publisher(server, publisher_id)
      when is_integer(publisher_id) and
             publisher_id <= 255 do
    GenServer.call(server, {:delete_publisher, publisher_id: publisher_id})
  end

  def query_metadata(server, streams)
      when is_list(streams) and
             length(streams) > 0 do
    GenServer.call(server, {:query_metadata, streams: streams})
  end

  def query_publisher_sequence(server, stream_name, publisher_reference)
      when is_binary(publisher_reference) and
             is_binary(stream_name) do
    GenServer.call(
      server,
      {:query_publisher_sequence, publisher_reference: publisher_reference, stream_name: stream_name}
    )
  end

  def publish(server, publisher_id, publishing_id, message, filter_value \\ nil)
      when is_integer(publisher_id) and
             is_binary(message) and
             is_integer(publishing_id) and
             (filter_value == nil or is_binary(filter_value)) and
             publisher_id <= 255 do
    GenServer.cast(
      server,
      {:publish, publisher_id: publisher_id, messages: [{publishing_id, message, filter_value}]}
    )
  end

  def subscribe(server, stream_name, pid, offset, credit, properties \\ [])
      when is_binary(stream_name) and
             is_integer(credit) and
             is_offset(offset) and
             is_list(properties) and
             is_pid(pid) and
             credit >= 0 do
    GenServer.call(
      server,
      {:subscribe, stream_name: stream_name, pid: pid, offset: offset, credit: credit, properties: properties}
    )
  end

  def unsubscribe(server, subscription_id) when subscription_id <= 255 do
    GenServer.call(server, {:unsubscribe, subscription_id: subscription_id})
  end

  def credit(server, subscription_id, credit) when is_integer(subscription_id) and credit >= 0 do
    GenServer.cast(server, {:credit, subscription_id: subscription_id, credit: credit})
  end

  def route(server, routing_key, super_stream) when is_binary(routing_key) and is_binary(super_stream) do
    GenServer.call(server, {:route, routing_key: routing_key, super_stream: super_stream})
  end

  def stream_stats(server, stream_name) when is_binary(stream_name) do
    GenServer.call(server, {:stream_stats, stream_name: stream_name})
  end

  def partitions(server, super_stream) when is_binary(super_stream) do
    GenServer.call(server, {:partitions, super_stream: super_stream})
  end

  def create_super_stream(server, name, partitions, binding_keys, arguments \\ [])
      when is_binary(name) and
             is_list(partitions) and
             length(partitions) > 0 and
             is_list(binding_keys) and
             length(binding_keys) > 0 do
    GenServer.call(
      server,
      {:create_super_stream, name: name, partitions: partitions, binding_keys: binding_keys, arguments: arguments}
    )
  end

  def delete_super_stream(server, name) when is_binary(name) do
    GenServer.call(server, {:delete_super_stream, name: name})
  end

  def respond(server, request, opts) when is_list(opts) do
    GenServer.cast(server, {:respond, request, opts})
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
  @type t() :: %RabbitMQStream.Connection{
          options: connection_options,
          socket: :gen_tcp.socket(),
          state: :closed | :connecting | :open | :closing,
          correlation_sequence: non_neg_integer(),
          publisher_sequence: non_neg_integer(),
          subscriber_sequence: non_neg_integer(),
          peer_properties: %{String.t() => term()},
          connection_properties: %{String.t() => String.t() | integer()},
          mechanisms: [String.t()],
          connect_requests: [pid()],
          request_tracker: %{{atom(), integer()} => {pid(), any()}},
          subscriptions: %{non_neg_integer() => pid()},
          commands: %{
            RabbitMQStream.Message.Helpers.command() => %{min: non_neg_integer(), max: non_neg_integer()}
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
    commands: %{},
    request_buffer: :queue.new(),
    frames_buffer: RabbitMQStream.Message.Buffer.init(),
    commands_buffer: :queue.new(),
    close_reason: nil
  ]
end
