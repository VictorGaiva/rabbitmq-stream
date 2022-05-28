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

  alias RabbitMQStream.Message.Command.{
    SaslHandshake,
    PeerProperties,
    SaslAuthenticate,
    Close,
    Tune,
    Open,
    Heartbeat,
    Create,
    Delete,
    StoreOffset,
    QueryOffset,
    DeclarePublisher,
    DeletePublisher,
    MetadataUpdate,
    QueryMetadata,
    QueryPublisherSequence,
    Publish,
    PublishConfirm,
    PublishError
  }

  alias Message.Code.{
    Ok,
    SaslMechanismNotSupported,
    AuthenticationFailure,
    SaslError,
    SaslChallenge,
    SaslAuthenticationFailureLoopback,
    VirtualHostAccessFailure
  }

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

  def start_link(default \\ []) when is_list(default) do
    GenServer.start_link(__MODULE__, default)
  end

  def connect(pid) do
    GenServer.call(pid, {:connect})
  end

  def close(pid, reason \\ "", code \\ 0x00) do
    GenServer.call(pid, {:close, reason, code})
  end

  def create_stream(pid, name, opts \\ []) when is_binary(name) do
    GenServer.call(pid, {:create, name, opts})
  end

  def delete_stream(pid, name) when is_binary(name) do
    GenServer.call(pid, {:delete, name})
  end

  def store_offset(pid, stream_name, offset_reference, offset)
      when is_binary(stream_name)
      when is_binary(offset_reference)
      when is_integer(offset)
      when length(stream_name) <= 255 do
    GenServer.call(pid, {:store_offset, stream_name: stream_name, offset_reference: offset_reference, offset: offset})
  end

  def query_offset(pid, stream_name, offset_reference)
      when is_binary(stream_name)
      when is_binary(offset_reference) do
    GenServer.call(pid, {:query_offset, stream_name: stream_name, offset_reference: offset_reference})
  end

  def declare_publisher(pid, stream_name, publisher_reference)
      when is_binary(publisher_reference)
      when is_binary(stream_name)
      when length(stream_name) <= 255 do
    GenServer.call(pid, {:declare_publisher, stream_name: stream_name, publisher_reference: publisher_reference})
  end

  def delete_publisher(pid, id)
      when is_integer(id)
      when id <= 255 do
    GenServer.call(pid, {:delete_publisher, id: id})
  end

  def query_metadata(pid, streams) do
    GenServer.call(pid, {:query_metadata, streams: streams})
  end

  def query_publisher_sequence(pid, stream_name, publisher_reference)
      when is_binary(publisher_reference)
      when is_binary(stream_name)
      when length(stream_name) <= 255 do
    GenServer.call(pid, {:query_publisher_sequence, publisher_reference: publisher_reference, stream_name: stream_name})
  end

  def publish(pid, publisher_id, message, publishing_id)
      when is_integer(publisher_id)
      when is_binary(message)
      when is_integer(publishing_id)
      when publisher_id <= 255 do
    GenServer.cast(
      pid,
      {:publish, publisher_id: publisher_id, published_messages: [{publishing_id, message}], wait: true}
    )
  end

  def get_state(pid) do
    GenServer.call(pid, {:get_state})
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
        |> send_request(%PeerProperties{})

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
      |> push_request_tracker(%Close{}, from)
      |> send_request(%Close{}, reason: reason, code: code)

    {:noreply, conn}
  end

  def handle_call({:create, name, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(%Create{}, from)
      |> send_request(%Create{}, name: name, arguments: opts)

    {:noreply, conn}
  end

  def handle_call({:delete, name}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(%Delete{}, from)
      |> send_request(%Delete{}, name: name)

    {:noreply, conn}
  end

  def handle_call({:store_offset, opts}, _from, %Connection{} = conn) do
    conn =
      conn
      |> send_request(%StoreOffset{}, opts)

    {:reply, :ok, conn}
  end

  def handle_call({:query_offset, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(%QueryOffset{}, from)
      |> send_request(%QueryOffset{}, opts)

    {:noreply, conn}
  end

  def handle_call({:declare_publisher, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(%DeclarePublisher{}, from, conn.publisher_sequence)
      |> send_request(%DeclarePublisher{}, opts ++ [publisher_sum: 1])

    {:noreply, conn}
  end

  def handle_call({:delete_publisher, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(%DeletePublisher{}, from)
      |> send_request(%DeletePublisher{}, opts)

    {:noreply, conn}
  end

  def handle_call({:query_metadata, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(%QueryMetadata{}, from)
      |> send_request(%QueryMetadata{}, opts)

    {:noreply, conn}
  end

  def handle_call({:query_publisher_sequence, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> push_request_tracker(%QueryPublisherSequence{}, from)
      |> send_request(%QueryPublisherSequence{}, opts)

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
      |> send_request(%Publish{}, opts ++ [correlation_sum: 0])

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

        %Response{code: %Ok{}} = decoded, conn ->
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
    conn = send_request(conn, %Heartbeat{}, correlation_sum: 0)

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
        |> send_request(%PeerProperties{})

      {:noreply, conn}
    else
      _ ->
        Logger.error("Failed to connect to #{conn.host}:#{conn.port}")
        {:noreply, conn}
    end
  end

  defp handle_message(%Connection{} = conn, %Response{command: %Close{}} = response) do
    Logger.debug("Connection closed: #{conn.host}:#{conn.port}")

    {{pid, _data}, conn} = pop_request_tracker(conn, %Close{}, response.correlation_id)

    conn = %{conn | state: :closed, socket: nil}

    GenServer.reply(pid, :ok)

    conn
  end

  defp handle_message(%Connection{} = conn, %Request{command: %Close{}} = request) do
    Logger.debug("Connection close requested by server: #{request.data.code} #{request.data.reason}")
    Logger.debug("Connection closed")

    %{conn | state: :closing}
    |> send_response(:close, correlation_id: request.correlation_id, code: %Ok{})
    |> handle_closed(request.data.reason)
  end

  defp handle_message(%Connection{state: :closed} = conn, _) do
    Logger.error("Message received on a closed connection")

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: %PeerProperties{}} = request) do
    Logger.debug("Exchange successful.")
    Logger.debug("Initiating SASL handshake.")

    %{conn | peer_properties: request.data.peer_properties}
    |> send_request(%SaslHandshake{})
  end

  defp handle_message(%Connection{} = conn, %Response{command: %SaslHandshake{}} = request) do
    Logger.debug("SASL handshake successful. Initiating authentication.")

    %{conn | mechanisms: request.data.mechanisms}
    |> send_request(%SaslAuthenticate{})
  end

  defp handle_message(%Connection{} = conn, %Response{command: %SaslAuthenticate{}, data: %{sasl_opaque_data: ""}}) do
    Logger.debug("Authentication successful. Initiating connection tuning.")

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: %SaslAuthenticate{}}) do
    Logger.debug("Authentication successful. Skipping connection tuning.")
    Logger.debug("Opening connection to vhost: \"#{conn.vhost}\"")

    conn
    |> send_request(%Open{})
    |> Map.put(:state, :opening)
  end

  defp handle_message(%Connection{} = conn, %Response{command: %Tune{}} = response) do
    Logger.debug("Tunning complete. Starting heartbeat timer.")

    Process.send_after(self(), {:heartbeat}, conn.heartbeat * 1000)

    %{conn | frame_max: response.data.frame_max, heartbeat: response.data.heartbeat}
  end

  defp handle_message(%Connection{} = conn, %Request{command: %Tune{}} = request) do
    Logger.debug("Tunning data received. Starting heartbeat timer.")
    Logger.debug("Opening connection to vhost: \"#{conn.vhost}\"")

    %{conn | frame_max: request.data.frame_max, heartbeat: request.data.heartbeat}
    |> send_response(:tune, correlation_id: request.correlation_id)
    |> Map.put(:state, :opening)
    |> send_request(%Open{})
  end

  defp handle_message(%Connection{} = conn, %Response{command: %Open{}} = response) do
    Logger.debug("Successfully opened connection with vhost: \"#{conn.vhost}\"")

    for request <- conn.connect_requests do
      GenServer.reply(request, :ok)
    end

    %{conn | state: :open, connect_requests: [], connection_properties: response.data.connection_properties}
  end

  defp handle_message(%Connection{} = conn, %Request{command: %Heartbeat{}}) do
    conn
  end

  defp handle_message(%Connection{} = conn, %Request{command: %MetadataUpdate{}} = request) do
    conn
    |> send_request(%QueryMetadata{}, streams: [request.data.stream_name])
  end

  defp handle_message(%Connection{} = conn, %Response{command: %QueryMetadata{}} = response) do
    metadata =
      response.data.streams
      |> Enum.map(&{&1.name, &1})
      |> Map.new()

    {{pid, _data}, conn} = pop_request_tracker(conn, %QueryMetadata{}, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data})
    end

    %{conn | metadata: Map.merge(conn.metadata, metadata)}
  end

  defp handle_message(%Connection{} = conn, %Response{command: %QueryOffset{}} = response) do
    {{pid, _data}, conn} = pop_request_tracker(conn, %QueryOffset{}, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data.offset})
    end

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: %DeclarePublisher{}} = response) do
    {{pid, id}, conn} = pop_request_tracker(conn, %DeclarePublisher{}, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, id})
    end

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: %QueryPublisherSequence{}} = response) do
    {{pid, _data}, conn} = pop_request_tracker(conn, %QueryPublisherSequence{}, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data.sequence})
    end

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: command} = response)
       when command in [%Create{}, %Delete{}, %DeletePublisher{}] do
    {{pid, _data}, conn} = pop_request_tracker(conn, command, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, :ok)
    end

    conn
  end

  defp handle_message(%Connection{} = conn, %Request{command: command} = _response)
       when command in [%PublishConfirm{}, %PublishError{}] do
    conn
  end

  defp handle_error(%Connection{} = conn, %Response{code: code})
       when code in [
              %SaslMechanismNotSupported{},
              %AuthenticationFailure{},
              %SaslError{},
              %SaslChallenge{},
              %SaslAuthenticationFailureLoopback{},
              %VirtualHostAccessFailure{}
            ] do
    Logger.error("Failed to connect to #{conn.host}:#{conn.port}. Reason: #{code.__struct__}")

    for request <- conn.connect_requests do
      GenServer.reply(request, {:error, code})
    end

    %{conn | state: :closed, socket: nil, connect_requests: []}
  end

  defp handle_error(%Connection{} = conn, %Response{command: command} = response)
       when command in [
              %Create{},
              %Delete{},
              %QueryOffset{},
              %DeclarePublisher{},
              %DeletePublisher{}
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

    frame = Request.new_encoded!(conn, command, opts)
    :ok = :gen_tcp.send(conn.socket, frame)

    correlation_sequence = conn.correlation_sequence + correlation_sum
    publisher_sequence = conn.publisher_sequence + publisher_sum

    %{conn | correlation_sequence: correlation_sequence, publisher_sequence: publisher_sequence}
  end

  defp send_response(%Connection{} = conn, command, opts) do
    frame = Response.new_encoded!(conn, command, opts)
    :ok = :gen_tcp.send(conn.socket, frame)

    conn
  end

  defp push_request_tracker(%Connection{} = conn, type, from, data \\ nil) when is_struct(type) when is_pid(from) do
    request_tracker = Map.put(conn.request_tracker, {type, conn.correlation_sequence}, {from, data})

    %{conn | request_tracker: request_tracker}
  end

  defp pop_request_tracker(%Connection{} = conn, type, correlation) when is_struct(type) do
    {entry, request_tracker} = Map.pop(conn.request_tracker, {type, correlation}, {nil, nil})

    {entry, %{conn | request_tracker: request_tracker}}
  end
end
