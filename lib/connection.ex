defmodule RabbitStream.Connection do
  use GenServer
  require Logger

  alias __MODULE__, as: Connection

  alias RabbitStream.Message
  alias RabbitStream.Message.{Request, Response}

  alias RabbitStream.Message.Command.Code.{
    SaslHandshake,
    PeerProperties,
    SaslAuthenticate,
    Close,
    Tune,
    Open,
    Heartbeat,
    Create
  }

  alias Response.Code.{
    Ok,
    SaslMechanismNotSupported,
    AuthenticationFailure,
    SaslError,
    SaslChallenge,
    SaslAuthenticationFailureLoopback,
    VirtualHostAccessFailure,
    StreamAlreadyExists
  }

  defstruct [
    :host,
    :vhost,
    :port,
    :username,
    :password,
    :socket,
    frame_max: 1_048_576,
    heartbeat: 60,
    version: 1,
    state: "closed",
    correlation: 1,
    peer_properties: [],
    connection_properties: [],
    mechanisms: [],
    connect_request: nil,
    close_request: nil,
    requests: %{}
  ]

  # @states [
  #   "connecting",
  #   "closed",
  #   "closing",
  #   "open",
  #   "opening"
  # ]

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
    GenServer.call(pid, {:create_stream, name, opts})
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

    conn = %Connection{
      host: host,
      port: port,
      vhost: vhost,
      username: username,
      password: password
    }

    {:ok, conn}
  end

  @impl true
  def handle_call({:get_state}, _from, %Connection{} = conn) do
    {:reply, conn, conn}
  end

  def handle_call({:connect}, from, %Connection{state: "closed"} = conn) do
    Logger.info("Connecting to server: #{conn.host}:#{conn.port}")

    with {:ok, socket} <- :gen_tcp.connect(String.to_charlist(conn.host), conn.port, [:binary, active: true]),
         :ok <- :gen_tcp.controlling_process(socket, self()) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        %{conn | socket: socket, state: "connecting", connect_request: from}
        |> send_request(:peer_properties)

      {:noreply, conn}
    else
      err ->
        Logger.error("Failed to connect to #{conn.host}:#{conn.port}")
        {:reply, {:error, err}, conn}
    end
  end

  def handle_call({:connect}, _from, %Connection{} = conn) do
    {:reply, {:error, "Can only connect while in the \"closed\" state. Current state: \"#{conn.state}\""}, conn}
  end

  def handle_call({:close, reason, code}, from, %Connection{state: "open"} = conn) do
    Logger.info("Connection close requested by client: #{reason} #{code}")

    conn =
      %{conn | state: "closing", close_request: from}
      |> send_request(:close, reason: reason, code: code)

    {:noreply, conn}
  end

  def handle_call({:close, _, _}, _from, %Connection{state: state} = conn) do
    {:reply, {:error, "Can't attempt to close a not \"open\" connection. Current state: \"#{state}\""}, conn}
  end

  def handle_call({:create_stream, name, opts}, from, %Connection{state: "open"} = conn) do
    conn =
      conn
      |> push_tracker(:create_stream, from)
      |> send_request(:create_stream, name: name, arguments: opts)

    {:noreply, conn}
  end

  @impl true
  def handle_info({:tcp, _socket, data}, conn) do
    conn =
      case Message.decode!(data) do
        %Request{} = decoded ->
          handle_message(conn, decoded)

        %Response{code: %Ok{}} = decoded ->
          handle_message(conn, decoded)

        decoded ->
          handle_error(conn, decoded)
      end

    case conn.state do
      "closed" ->
        {:noreply, conn, :hibernate}

      _ ->
        {:noreply, conn}
    end
  end

  def handle_info({:heartbeat}, conn) do
    conn = send_request(conn, :heartbeat, sum: 0)

    Process.send_after(self(), {:heartbeat}, conn.heartbeat * 1000)

    {:noreply, conn}
  end

  def handle_info({:tcp_closed, _socket}, conn) do
    if conn.state == "connecting" do
      Logger.warn(
        "The connection was closed by the host, after the socket was already open, while running the authentication sequence. This could be caused by the server not having Stream Plugin active"
      )
    end

    conn = %{conn | socket: nil, state: "closed"} |> handle_closed(:tcp_closed)

    {:noreply, conn, :hibernate}
  end

  def handle_info({:tcp_error, _socket, reason}, conn) do
    conn = %{conn | socket: nil, state: "closed"} |> handle_closed(reason)

    {:noreply, conn}
  end

  defp handle_message(conn, %Response{command: %Close{}}) do
    Logger.debug("Connection closed: #{conn.host}:#{conn.port}")

    client = conn.close_request

    conn = %{conn | state: "closed", socket: nil, close_request: nil}

    GenServer.reply(client, {:ok, conn})

    conn
  end

  defp handle_message(%Connection{} = conn, %Request{command: %Close{}} = request) do
    Logger.debug("Connection close requested by server: #{request.data.code} #{request.data.reason}")
    Logger.debug("Connection closed")

    %{conn | state: "closing"}
    |> send_response(:close, correlation_id: request.correlation_id, code: %Response.Code.Ok{})
    |> handle_closed(request.data.reason)
  end

  defp handle_message(%Connection{state: "closed"} = conn, _) do
    Logger.error("Message received on a closed connection")

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: %PeerProperties{}} = request) do
    Logger.debug("Exchange successful.")
    Logger.debug("Initiating SASL handshake.")

    %{conn | peer_properties: request.data.peer_properties}
    |> send_request(:sasl_handshake)
  end

  defp handle_message(%Connection{} = conn, %Response{command: %SaslHandshake{}} = request) do
    Logger.debug("SASL handshake successful. Initiating authentication.")

    %{conn | mechanisms: request.data.mechanisms}
    |> send_request(:sasl_authenticate)
  end

  defp handle_message(%Connection{} = conn, %Response{command: %SaslAuthenticate{}, data: %{sasl_opaque_data: ""}}) do
    Logger.debug("Authentication successful. Initiating connection tuning.")

    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: %SaslAuthenticate{}}) do
    Logger.debug("Authentication successful. Skipping connection tuning.")
    Logger.debug("Opening connection to vhost: \"#{conn.vhost}\"")

    conn
    |> send_request(:open)
    |> Map.put(:state, "opening")
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
    |> Map.put(:state, "opening")
    |> send_request(:open)
  end

  defp handle_message(%Connection{} = conn, %Response{command: %Open{}} = response) do
    Logger.debug("Successfully opened connection with vhost: \"#{conn.vhost}\"")

    client = conn.connect_request

    conn = %{conn | state: "open", connect_request: nil, connection_properties: response.data.connection_properties}

    GenServer.reply(client, {:ok, conn})

    conn
  end

  defp handle_message(%Connection{} = conn, %Request{command: %Heartbeat{}}) do
    conn
  end

  defp handle_message(%Connection{} = conn, %Response{command: %Create{}} = response) do
    {client, conn} = pop_tracker(conn, :create_stream, response.correlation_id)

    if client != nil do
      GenServer.reply(client, :ok)
    end

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

    GenServer.reply(conn.connect_request, {:error, code})

    %{conn | state: "closed", socket: nil, connect_request: nil}
  end

  defp handle_error(%Connection{} = conn, %Response{code: %StreamAlreadyExists{}} = response) do
    {client, conn} = pop_tracker(conn, :create_stream, response.correlation_id)

    if client != nil do
      GenServer.reply(client, {:error, response.code})
    end

    conn
  end

  defp handle_closed(%Connection{} = conn, reason) do
    if conn.close_request != nil do
      GenServer.reply(conn.close_request, {:error, reason})
    end

    for client <- Map.values(conn.requests) do
      GenServer.reply(client, {:error, reason})
    end

    %{conn | requests: %{}, close_request: nil}
  end

  defp send_request(%Connection{} = conn, command, opts \\ []) do
    frame = Request.new_encoded!(conn, command, opts)
    :ok = :gen_tcp.send(conn.socket, frame)

    %{conn | correlation: conn.correlation + (opts[:sum] || 1)}
  end

  defp send_response(%Connection{} = conn, command, opts) do
    frame = Response.new_encoded!(conn, command, opts)
    :ok = :gen_tcp.send(conn.socket, frame)

    conn
  end

  defp push_tracker(%Connection{} = conn, type, from) when is_atom(type) when is_pid(from) do
    requests = Map.put(conn.requests, {type, conn.correlation}, from)

    %{conn | requests: requests}
  end

  defp pop_tracker(%Connection{} = conn, type, correlation) when is_atom(type) do
    {value, requests} = Map.pop(conn.requests, {type, correlation})

    if value == nil do
      Logger.error("No pending request for \"#{type}:#{correlation}\" found.")
    end

    {value, %{conn | requests: requests}}
  end
end
