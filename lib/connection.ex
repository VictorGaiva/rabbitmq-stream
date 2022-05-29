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
  alias Message.{Request, Response}

  defstruct [
    :host,
    :vhost,
    :port,
    :username,
    :password,
    :socket,
    :frame_max,
    :heartbeat,
    version: 1,
    correlation_sequence: 1,
    publisher_sequence: 1,
    subscription_sequence: 1,
    state: :closed,
    peer_properties: [],
    connection_properties: [],
    mechanisms: [],
    connect_requests: [],
    publish_tracker: %PublishingTracker{},
    request_tracker: %{},
    metadata: %{}
  ]

  @type t() :: %Connection{
          host: String.t(),
          vhost: String.t(),
          port: integer(),
          username: String.t(),
          password: String.t(),
          socket: :gen_tcp.socket(),
          frame_max: integer(),
          heartbeat: integer(),
          version: 1,
          state: :closed | :connecting | :open | :closing,
          correlation_sequence: integer(),
          publisher_sequence: integer(),
          subscription_sequence: integer(),
          peer_properties: [[String.t()]],
          connection_properties: [[String.t()]],
          mechanisms: [String.t()],
          connect_requests: [pid()],
          publish_tracker: %PublishingTracker{},
          request_tracker: %{{struct(), integer()} => {pid(), any()}},
          metadata: %{String.t() => any()}
        }

  def start_link(args \\ []) when is_list(args) do
    GenServer.start_link(__MODULE__, args, name: args[:name])
  end

  @spec connect(GenServer.server()) :: :ok | {:error, reason :: atom()}
  def connect(pid) do
    GenServer.call(pid, {:connect})
  end

  @spec close(GenServer.server(), reason :: String.t(), code :: integer()) :: :ok | {:error, reason :: atom()}
  def close(pid, reason \\ "", code \\ 0x00) do
    GenServer.call(pid, {:close, reason, code})
  end

  @spec create_stream(GenServer.server(), String.t(), keyword(String.t())) :: :ok | {:error, reason :: atom()}
  def create_stream(pid, name, arguments \\ []) when is_binary(name) do
    GenServer.call(pid, {:create, name, arguments})
  end

  @spec delete_stream(GenServer.server(), String.t()) :: :ok | {:error, reason :: atom()}
  def delete_stream(pid, name) when is_binary(name) do
    GenServer.call(pid, {:delete, name})
  end

  @spec store_offset(GenServer.server(), String.t(), String.t(), integer()) :: :ok
  def store_offset(pid, stream_name, offset_reference, offset)
      when is_binary(stream_name)
      when is_binary(offset_reference)
      when is_integer(offset)
      when length(stream_name) <= 255 do
    GenServer.call(pid, {:store_offset, stream_name: stream_name, offset_reference: offset_reference, offset: offset})
  end

  @spec query_offset(GenServer.server(), String.t(), String.t()) ::
          {:ok, offset :: integer()} | {:error, reason :: atom()}
  def query_offset(pid, stream_name, offset_reference)
      when is_binary(offset_reference)
      when is_binary(stream_name)
      when length(stream_name) <= 255 do
    GenServer.call(pid, {:query_offset, stream_name: stream_name, offset_reference: offset_reference})
  end

  @spec declare_publisher(GenServer.server(), String.t(), String.t()) ::
          {:ok, publisher_id :: integer()} | {:error, any()}
  def declare_publisher(pid, stream_name, publisher_reference)
      when is_binary(publisher_reference)
      when is_binary(stream_name)
      when length(stream_name) <= 255 do
    GenServer.call(pid, {:declare_publisher, stream_name: stream_name, publisher_reference: publisher_reference})
  end

  @spec delete_publisher(GenServer.server(), publisher_id :: integer()) ::
          :ok | {:error, reason :: atom()}
  def delete_publisher(pid, publisher_id)
      when is_integer(publisher_id)
      when publisher_id <= 255 do
    GenServer.call(pid, {:delete_publisher, publisher_id: publisher_id})
  end

  @spec query_metadata(GenServer.server(), [String.t(), ...]) ::
          {:ok, metadata :: %{String.t() => String.t()}} | {:error, reason :: atom()}
  def query_metadata(pid, streams)
      when is_list(streams)
      when length(streams) > 0 do
    GenServer.call(pid, {:query_metadata, streams: streams})
  end

  @spec query_publisher_sequence(GenServer.server(), String.t(), String.t()) ::
          {:ok, sequence :: integer()} | {:error, reason :: atom()}
  def query_publisher_sequence(pid, stream_name, publisher_reference)
      when is_binary(publisher_reference)
      when is_binary(stream_name)
      when length(stream_name) <= 255 do
    GenServer.call(pid, {:query_publisher_sequence, publisher_reference: publisher_reference, stream_name: stream_name})
  end

  @spec publish(GenServer.server(), integer(), integer(), binary()) :: :ok
  def publish(pid, publisher_id, publishing_id, message)
      when is_integer(publisher_id)
      when is_binary(message)
      when is_integer(publishing_id)
      when publisher_id <= 255 do
    GenServer.cast(
      pid,
      {:publish, publisher_id: publisher_id, published_messages: [{publishing_id, message}], wait: true}
    )
  end

  @impl true
  def init(opts \\ []) do
    username = opts[:username] || "guest"
    password = opts[:password] || "guest"
    host = opts[:host] || "localhost"
    port = opts[:port] || 5552
    vhost = opts[:vhost] || "/"
    frame_max = opts[:frame_max] || 1_048_576
    heartbeat = opts[:heartbeat] || 60

    conn = %Connection{
      host: host,
      port: port,
      vhost: vhost,
      username: username,
      password: password,
      frame_max: frame_max,
      heartbeat: heartbeat
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
    Logger.info("Connecting to server: #{conn.host}:#{conn.port}")

    with {:ok, socket} <- :gen_tcp.connect(String.to_charlist(conn.host), conn.port, [:binary, active: true]),
         :ok <- :gen_tcp.controlling_process(socket, self()) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        %{conn | socket: socket, state: :connecting, connect_requests: [from]}
        |> send_request(:peer_properties)

      {:noreply, conn}
    else
      err ->
        Logger.error("Failed to connect to #{conn.host}:#{conn.port}")
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
      |> push_request_tracker(:close, from)
      |> send_request(:close, reason: reason, code: code)

    {:noreply, conn}
  end

  def handle_call({:create, name, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(:create, from)
      |> send_request(:create, name: name, arguments: opts)

    {:noreply, conn}
  end

  def handle_call({:delete, name}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(:delete, from)
      |> send_request(:delete, name: name)

    {:noreply, conn}
  end

  def handle_call({:store_offset, opts}, _from, %Connection{} = conn) do
    conn =
      conn
      |> send_request(:store_offset, opts)

    {:reply, :ok, conn}
  end

  def handle_call({:query_offset, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(:query_offset, from)
      |> send_request(:query_offset, opts)

    {:noreply, conn}
  end

  def handle_call({:declare_publisher, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(:declare_publisher, from, conn.publisher_sequence)
      |> send_request(:declare_publisher, opts ++ [publisher_sum: 1])

    {:noreply, conn}
  end

  def handle_call({:delete_publisher, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(:delete_publisher, from)
      |> send_request(:delete_publisher, opts)

    {:noreply, conn}
  end

  def handle_call({:query_metadata, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(:query_metadata, from)
      |> send_request(:query_metadata, opts)

    {:noreply, conn}
  end

  def handle_call({:query_publisher_sequence, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(:query_publisher_sequence, from)
      |> send_request(:query_publisher_sequence, opts)

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
      |> send_request(:publish, opts ++ [correlation_sum: 0])

    {:noreply, conn}
  end

  @impl true
  def handle_info({:tcp, _socket, data}, conn) do
    conn =
      data
      |> Message.decode!()
      |> Enum.reduce(conn, fn
        %Request{} = decoded, conn ->
          handle_message(conn, decoded)

        %Response{code: :ok} = decoded, conn ->
          handle_message(conn, decoded)

        %Response{code: nil} = decoded, conn ->
          handle_message(conn, decoded)

        decoded, conn ->
          handle_error(conn, decoded)
      end)

    cond do
      conn.state == :closed ->
        {:noreply, conn, :hibernate}

      true ->
        {:noreply, conn}
    end
  end

  def handle_info({:heartbeat}, conn) do
    conn = send_request(conn, :heartbeat, correlation_sum: 0)

    Process.send_after(self(), {:heartbeat}, conn.heartbeat * 1000)

    {:noreply, conn}
  end

  def handle_info({:tcp_closed, _socket}, conn) do
    if conn.state == :connecting do
      Logger.warn(
        "The connection was closed by the host, after the socket was already open, while running the authentication sequence. This could be caused by the server not having Stream Plugin active"
      )
    end

    conn = %{conn | socket: nil, state: :closed} |> handle_closed(:tcp_closed)

    {:noreply, conn, :hibernate}
  end

  def handle_info({:tcp_error, _socket, reason}, conn) do
    conn = %{conn | socket: nil, state: :closed} |> handle_closed(reason)

    {:noreply, conn}
  end

  @impl true
  def handle_continue({:connect}, %Connection{state: :closed} = conn) do
    Logger.info("Connecting to server: #{conn.host}:#{conn.port}")

    with {:ok, socket} <- :gen_tcp.connect(String.to_charlist(conn.host), conn.port, [:binary, active: true]),
         :ok <- :gen_tcp.controlling_process(socket, self()) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        %{conn | socket: socket, state: :connecting}
        |> send_request(:peer_properties)

      {:noreply, conn}
    else
      _ ->
        Logger.error("Failed to connect to #{conn.host}:#{conn.port}")
        {:noreply, conn}
    end
  end

  defp handle_message(%Connection{} = conn, %Response{command: :close} = response) do
    Logger.debug("Connection closed: #{conn.host}:#{conn.port}")

    {{pid, _data}, conn} = pop_request_tracker(conn, :close, response.correlation_id)

    conn = %{conn | state: :closed, socket: nil}

    GenServer.reply(pid, :ok)

    conn
  end

  defp handle_message(%Connection{} = conn, %Request{command: :close} = request) do
    Logger.debug("Connection close requested by server: #{request.data.code} #{request.data.reason}")
    Logger.debug("Connection closed")

    %{conn | state: :closing}
    |> send_response(:close, correlation_id: request.correlation_id, code: :ok)
    |> handle_closed(request.data.reason)
  end

  defp handle_message(%Connection{state: :closed} = conn, _) do
    Logger.error("Message received on a closed connection")

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: :peer_properties} = request) do
    Logger.debug("Exchange successful.")
    Logger.debug("Initiating SASL handshake.")

    %{conn | peer_properties: request.data.peer_properties}
    |> send_request(:sasl_handshake)
  end

  defp handle_message(%Connection{} = conn, %Response{command: :sasl_handshake} = request) do
    Logger.debug("SASL handshake successful. Initiating authentication.")

    %{conn | mechanisms: request.data.mechanisms}
    |> send_request(:sasl_authenticate)
  end

  defp handle_message(%Connection{} = conn, %Response{command: :sasl_authenticate, data: %{sasl_opaque_data: ""}}) do
    Logger.debug("Authentication successful. Initiating connection tuning.")

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: :sasl_authenticate}) do
    Logger.debug("Authentication successful. Skipping connection tuning.")
    Logger.debug("Opening connection to vhost: \"#{conn.vhost}\"")

    conn
    |> send_request(:open)
    |> Map.put(:state, :opening)
  end

  defp handle_message(%Connection{} = conn, %Response{command: :tune} = response) do
    Logger.debug("Tunning complete. Starting heartbeat timer.")

    Process.send_after(self(), {:heartbeat}, conn.heartbeat * 1000)

    %{conn | frame_max: response.data.frame_max, heartbeat: response.data.heartbeat}
  end

  defp handle_message(%Connection{} = conn, %Request{command: :tune} = request) do
    Logger.debug("Tunning data received. Starting heartbeat timer.")
    Logger.debug("Opening connection to vhost: \"#{conn.vhost}\"")

    %{conn | frame_max: request.data.frame_max, heartbeat: request.data.heartbeat}
    |> send_response(:tune, correlation_id: request.correlation_id)
    |> Map.put(:state, :opening)
    |> send_request(:open)
  end

  defp handle_message(%Connection{} = conn, %Response{command: :open} = response) do
    Logger.debug("Successfully opened connection with vhost: \"#{conn.vhost}\"")

    for request <- conn.connect_requests do
      GenServer.reply(request, :ok)
    end

    %{conn | state: :open, connect_requests: [], connection_properties: response.data.connection_properties}
  end

  defp handle_message(%Connection{} = conn, %Request{command: :heartbeat}) do
    conn
  end

  defp handle_message(%Connection{} = conn, %Request{command: :metadata_update} = request) do
    conn
    |> send_request(:query_metadata, streams: [request.data.stream_name])
  end

  defp handle_message(%Connection{} = conn, %Response{command: :query_metadata} = response) do
    metadata =
      response.data.streams
      |> Enum.map(&{&1.name, &1})
      |> Map.new()

    {{pid, _data}, conn} = pop_request_tracker(conn, :query_metadata, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data})
    end

    %{conn | metadata: Map.merge(conn.metadata, metadata)}
  end

  defp handle_message(%Connection{} = conn, %Response{command: :query_offset} = response) do
    {{pid, _data}, conn} = pop_request_tracker(conn, :query_offset, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data.offset})
    end

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: :declare_publisher} = response) do
    {{pid, id}, conn} = pop_request_tracker(conn, :declare_publisher, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, id})
    end

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: :query_publisher_sequence} = response) do
    {{pid, _data}, conn} = pop_request_tracker(conn, :query_publisher_sequence, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data.sequence})
    end

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: command} = response)
       when command in [:create, :delete, :delete_publisher] do
    {{pid, _data}, conn} = pop_request_tracker(conn, command, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, :ok)
    end

    conn
  end

  defp handle_message(%Connection{} = conn, %Request{command: command} = _response)
       when command in [:publish_confirm, :publish_error] do
    conn
  end

  defp handle_error(%Connection{} = conn, %Response{code: code})
       when code in [
              :sasl_mechanism_not_supported,
              :authentication_failure,
              :sasl_error,
              :sasl_challenge,
              :sasl_authentication_failure_loopback,
              :virtual_host_access_failure
            ] do
    Logger.error("Failed to connect to #{conn.host}:#{conn.port}. Reason: #{code}")

    for request <- conn.connect_requests do
      GenServer.reply(request, {:error, code})
    end

    %{conn | state: :closed, socket: nil, connect_requests: []}
  end

  defp handle_error(%Connection{} = conn, %Response{command: command} = response)
       when command in [
              :create,
              :delete,
              :query_offset,
              :declare_publisher,
              :delete_publisher
            ] do
    {{pid, _data}, conn} = pop_request_tracker(conn, command, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:error, response.code})
    end

    conn
  end

  defp handle_closed(%Connection{} = conn, reason) do
    for request <- conn.connect_requests do
      GenServer.reply(request, {:error, :closed})
    end

    for {client, _data} <- Map.values(conn.request_tracker) do
      GenServer.reply(client, {:error, reason})
    end

    %{conn | request_tracker: %{}, connect_requests: []}
  end

  defp send_request(%Connection{} = conn, command, opts \\ []) do
    {correlation_sum, opts} = Keyword.pop(opts, :correlation_sum, 1)
    {publisher_sum, opts} = Keyword.pop(opts, :publisher_sum, 0)

    frame = Message.encode_request!(conn, command, opts)

    :ok = :gen_tcp.send(conn.socket, frame)

    correlation_sequence = conn.correlation_sequence + correlation_sum
    publisher_sequence = conn.publisher_sequence + publisher_sum

    %{conn | correlation_sequence: correlation_sequence, publisher_sequence: publisher_sequence}
  end

  defp send_response(%Connection{} = conn, command, opts) do
    frame = Message.encode_response!(conn, command, opts)

    :ok = :gen_tcp.send(conn.socket, frame)

    conn
  end

  defp push_request_tracker(%Connection{} = conn, type, from, data \\ nil) when is_atom(type) when is_pid(from) do
    request_tracker = Map.put(conn.request_tracker, {type, conn.correlation_sequence}, {from, data})

    %{conn | request_tracker: request_tracker}
  end

  defp pop_request_tracker(%Connection{} = conn, type, correlation) when is_atom(type) do
    {entry, request_tracker} = Map.pop(conn.request_tracker, {type, correlation}, {nil, nil})

    {entry, %{conn | request_tracker: request_tracker}}
  end

  if Mix.env() == :test do
    def get_state(pid) do
      GenServer.call(pid, {:get_state})
    end
  end
end
