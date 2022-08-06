defmodule RabbitMQStream.Connection do
  @moduledoc """
  Responsible for encoding and decoding messages, opening and maintaining a socket connection to a single node.
  It connects to the RabbitMQ server using [`:gen_tcp`](https://www.erlang.org/doc/man/gen_tcp.html).
  It then runs throught the [authentication](https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_stream/docs/PROTOCOL.adoc#authentication) sequence and mantains the connection open with heartbeats, with the provided `tune` definition.
  """

  use GenServer
  require Logger
  alias __MODULE__

  alias RabbitMQStream.Helpers.PublishingTracker

  alias RabbitMQStream.Message
  alias RabbitMQStream.Message.{Request, Response}
  alias RabbitMQStream.Connection.Handler

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

  @type t() :: %Connection{
          options: connection_options,
          socket: :gen_tcp.socket(),
          version: 1,
          state: :closed | :connecting | :open | :closing,
          correlation_sequence: integer(),
          publisher_sequence: non_neg_integer(),
          subscriber_sequence: non_neg_integer(),
          peer_properties: [[String.t()]],
          connection_properties: [[String.t()]],
          mechanisms: [String.t()],
          connect_requests: [pid()],
          publish_tracker: %PublishingTracker{},
          request_tracker: %{{atom(), integer()} => {pid(), any()}},
          metadata: %{String.t() => any()},
          subscriptions: %{non_neg_integer() => pid()}
        }

  @spec start_link([connection_option | {:name, atom()}]) :: :ignore | {:error, any} | {:ok, pid}

  @spec connect(conn :: GenServer.server()) :: :ok | {:error, reason :: atom()}

  @spec close(conn :: GenServer.server(), reason :: String.t(), code :: integer()) :: :ok | {:error, reason :: atom()}

  @spec create_stream(conn :: GenServer.server(), String.t(), keyword(String.t())) :: :ok | {:error, reason :: atom()}

  @spec delete_stream(conn :: GenServer.server(), String.t()) :: :ok | {:error, reason :: atom()}

  @spec store_offset(conn :: GenServer.server(), String.t(), String.t(), integer()) :: :ok

  @spec query_offset(conn :: GenServer.server(), String.t(), String.t()) ::
          {:ok, offset :: integer()} | {:error, reason :: atom()}

  @spec declare_publisher(conn :: GenServer.server(), String.t(), String.t()) ::
          {:ok, publisher_id :: integer()} | {:error, any()}

  @spec delete_publisher(conn :: GenServer.server(), publisher_id :: integer()) ::
          :ok | {:error, reason :: atom()}

  @spec query_metadata(conn :: GenServer.server(), [String.t(), ...]) ::
          {:ok, metadata :: %{String.t() => String.t()}} | {:error, reason :: atom()}

  @spec query_publisher_sequence(conn :: GenServer.server(), String.t(), String.t()) ::
          {:ok, sequence :: integer()} | {:error, reason :: atom()}

  @spec publish(conn :: GenServer.server(), integer(), integer(), binary()) :: :ok

  @spec subscribe(
          conn :: GenServer.server(),
          stream_name :: String.t(),
          pid :: pid(),
          offset :: offset(),
          credit :: non_neg_integer(),
          properties :: %{String.t() => String.t()}
        ) :: {:ok, subscription_id :: non_neg_integer()} | {:error, reason :: atom()}

  @spec unsubscribe(conn :: GenServer.server(), subscription_id :: non_neg_integer()) ::
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
    publish_tracker: %PublishingTracker{},
    request_tracker: %{},
    metadata: %{}
  ]

  def start_link(args \\ []) when is_list(args) do
    GenServer.start_link(__MODULE__, args, name: args[:name])
  end

  def connect(conn) do
    GenServer.call(conn, {:connect})
  end

  def close(conn, reason \\ "", code \\ 0x00) do
    GenServer.call(conn, {:close, reason, code})
  end

  def create_stream(conn, name, arguments \\ []) when is_binary(name) do
    GenServer.call(conn, {:create, arguments ++ [name: name]})
  end

  def delete_stream(conn, name) when is_binary(name) do
    GenServer.call(conn, {:delete, name: name})
  end

  def store_offset(conn, stream_name, offset_reference, offset)
      when is_binary(stream_name)
      when is_binary(offset_reference)
      when is_integer(offset)
      when length(stream_name) <= 255 do
    GenServer.call(conn, {:store_offset, stream_name: stream_name, offset_reference: offset_reference, offset: offset})
  end

  def query_offset(conn, stream_name, offset_reference)
      when is_binary(offset_reference)
      when is_binary(stream_name)
      when length(stream_name) <= 255 do
    GenServer.call(conn, {:query_offset, stream_name: stream_name, offset_reference: offset_reference})
  end

  def declare_publisher(conn, stream_name, publisher_reference)
      when is_binary(publisher_reference)
      when is_binary(stream_name)
      when length(stream_name) <= 255 do
    GenServer.call(conn, {:declare_publisher, stream_name: stream_name, publisher_reference: publisher_reference})
  end

  def delete_publisher(conn, publisher_id)
      when is_integer(publisher_id)
      when publisher_id <= 255 do
    GenServer.call(conn, {:delete_publisher, publisher_id: publisher_id})
  end

  def query_metadata(conn, streams)
      when is_list(streams)
      when length(streams) > 0 do
    GenServer.call(conn, {:query_metadata, streams: streams})
  end

  def query_publisher_sequence(conn, stream_name, publisher_reference)
      when is_binary(publisher_reference)
      when is_binary(stream_name)
      when length(stream_name) <= 255 do
    GenServer.call(
      conn,
      {:query_publisher_sequence, publisher_reference: publisher_reference, stream_name: stream_name}
    )
  end

  def publish(conn, publisher_id, publishing_id, message)
      when is_integer(publisher_id)
      when is_binary(message)
      when is_integer(publishing_id)
      when publisher_id <= 255 do
    GenServer.cast(
      conn,
      {:publish, publisher_id: publisher_id, published_messages: [{publishing_id, message}], wait: true}
    )
  end

  def subscribe(conn, stream_name, pid, offset, credit, properties \\ %{})
      when is_binary(stream_name)
      when is_integer(credit)
      when offset in [:first, :last, :next] or
             (tuple_size(offset) == 2 and is_tuple(offset) and elem(offset, 0) in [:offset, :timestamp])
      when is_map(properties)
      when is_pid(pid)
      when length(stream_name) <= 255
      when credit >= 0 do
    GenServer.call(
      conn,
      {:subscribe, stream_name: stream_name, pid: pid, offset: offset, credit: credit, properties: properties}
    )
  end

  def unsubscribe(conn, subscription_id) when subscription_id <= 255 do
    GenServer.call(conn, {:unsubscribe, subscription_id: subscription_id})
  end

  @impl true
  def init(opts \\ []) do
    conn = %Connection{
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

  @impl true
  def handle_call({:get_state}, _from, %Connection{} = conn) do
    {:reply, conn, conn}
  end

  def handle_call({:connect}, from, %Connection{state: :closed} = conn) do
    Logger.info("Connecting to server: #{conn.options[:host]}:#{conn.options[:port]}")

    with {:ok, socket} <-
           :gen_tcp.connect(String.to_charlist(conn.options[:host]), conn.options[:port], [:binary, active: true]),
         :ok <- :gen_tcp.controlling_process(socket, self()) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        %{conn | socket: socket, state: :connecting, connect_requests: [from]}
        |> Handler.send_request(:peer_properties)

      {:noreply, conn}
    else
      err ->
        Logger.error("Failed to connect to #{conn.options[:host]}:#{conn.options[:port]}")
        {:reply, {:error, err}, conn}
    end
  end

  def handle_call({:connect}, _from, %Connection{state: :open} = conn) do
    {:reply, :ok, conn}
  end

  def handle_call({:connect}, from, %Connection{} = conn) do
    conn = %{conn | connect_requests: conn.connect_requests ++ [from]}
    {:noreply, conn}
  end

  def handle_call(_, _from, %Connection{state: state} = conn) when state != :open do
    {:reply, {:error, state}, conn}
  end

  def handle_call({:close, reason, code}, from, %Connection{} = conn) do
    Logger.info("Connection close requested by client: #{reason} #{code}")

    conn =
      %{conn | state: :closing}
      |> Handler.push_request_tracker(:close, from)
      |> Handler.send_request(:close, reason: reason, code: code)

    {:noreply, conn}
  end

  def handle_call({:subscribe, opts}, from, %Connection{} = conn) do
    subscription_id = conn.subscriber_sequence

    conn =
      conn
      |> Handler.push_request_tracker(:subscribe, from, {subscription_id, opts[:pid]})
      |> Handler.send_request(:subscribe, opts ++ [subscriber_sum: 1, subscription_id: subscription_id])

    {:noreply, conn}
  end

  def handle_call({command, opts}, from, %Connection{} = conn)
      when command in [
             :query_offset,
             :delete_publisher,
             :query_metadata,
             :query_publisher_sequence,
             :delete,
             :create,
             :unsubscribe
           ] do
    conn =
      conn
      |> Handler.push_request_tracker(command, from)
      |> Handler.send_request(command, opts)

    {:noreply, conn}
  end

  def handle_call({:store_offset, opts}, _from, %Connection{} = conn) do
    conn =
      conn
      |> Handler.send_request(:store_offset, opts)

    {:reply, :ok, conn}
  end

  def handle_call({:declare_publisher, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> Handler.push_request_tracker(:declare_publisher, from, conn.publisher_sequence)
      |> Handler.send_request(:declare_publisher, opts ++ [publisher_sum: 1])

    {:noreply, conn}
  end

  @impl true
  def handle_cast({:publish, opts}, %Connection{} = conn) do
    {_wait, opts} = Keyword.pop(opts, :wait, false)

    # conn =
    #   if wait do
    #     publishing_ids = Enum.map(opts[:published_messages], fn {id, _} -> id end)
    #     publish_tracker = PublishingTracker.push(conn.publish_tracker, opts[:publisher_id], publishing_ids, from)
    #     %{conn | publish_tracker: publish_tracker}
    #   else
    #     conn
    #   end

    conn =
      conn
      |> Handler.send_request(:publish, opts ++ [correlation_sum: 0])

    {:noreply, conn}
  end

  @impl true
  def handle_info({:tcp, _socket, data}, conn) do
    conn =
      data
      |> Message.decode!()
      |> Enum.reduce(conn, fn
        %Request{} = decoded, conn ->
          Handler.handle_message(conn, decoded)

        %Response{code: :ok} = decoded, conn ->
          Handler.handle_message(conn, decoded)

        %Response{code: nil} = decoded, conn ->
          Handler.handle_message(conn, decoded)

        decoded, conn ->
          Handler.handle_error(conn, decoded)
      end)

    cond do
      conn.state == :closed ->
        {:noreply, conn, :hibernate}

      true ->
        {:noreply, conn}
    end
  end

  def handle_info({:heartbeat}, conn) do
    conn = Handler.send_request(conn, :heartbeat, correlation_sum: 0)

    Process.send_after(self(), {:heartbeat}, conn.options[:heartbeat] * 1000)

    {:noreply, conn}
  end

  def handle_info({:tcp_closed, _socket}, conn) do
    if conn.state == :connecting do
      Logger.warn(
        "The connection was closed by the host, after the socket was already open, while running the authentication sequence. This could be caused by the server not having Stream Plugin active"
      )
    end

    conn = %{conn | socket: nil, state: :closed} |> Handler.handle_closed(:tcp_closed)

    {:noreply, conn, :hibernate}
  end

  def handle_info({:tcp_error, _socket, reason}, conn) do
    conn = %{conn | socket: nil, state: :closed} |> Handler.handle_closed(reason)

    {:noreply, conn}
  end

  @impl true
  def handle_continue({:connect}, %Connection{state: :closed} = conn) do
    Logger.info("Connecting to server: #{conn.options[:host]}:#{conn.options[:port]}")

    with {:ok, socket} <-
           :gen_tcp.connect(String.to_charlist(conn.options[:host]), conn.options[:port], [:binary, active: true]),
         :ok <- :gen_tcp.controlling_process(socket, self()) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        %{conn | socket: socket, state: :connecting}
        |> Handler.send_request(:peer_properties)

      {:noreply, conn}
    else
      _ ->
        Logger.error("Failed to connect to #{conn.options[:host]}:#{conn.options[:port]}")
        {:noreply, conn}
    end
  end

  if Mix.env() == :test do
    def get_state(pid) do
      GenServer.call(pid, {:get_state})
    end
  end
end
