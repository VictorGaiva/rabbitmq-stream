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
    Heartbeat
  }

  alias Response.Code.{
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
    frame_max: 1_048_576,
    heartbeat: 60,
    version: 1,
    state: "closed",
    correlation: 1,
    peer_properties: [],
    connection_properties: [],
    mechanisms: [],
    requests: %{
      connect: nil,
      close: nil
    }
  ]

  @states [
    "connecting",
    "closed",
    "closing",
    "open",
    "opening"
  ]

  def start_link(default \\ []) when is_list(default) do
    GenServer.start_link(__MODULE__, default)
  end

  def connect(pid) do
    GenServer.call(pid, {:connect})
  end

  def close(pid, code, reason) do
    GenServer.call(pid, {:close, code, reason})
  end

  @impl true
  def init(opts \\ []) do
    username = opts[:username] || "guest"
    password = opts[:password] || "guest"
    host = opts[:host] || 'localhost'
    port = opts[:port] || 5552
    vhost = opts[:vhost] || "dev"

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
  def handle_call({:connect}, from, %Connection{state: "closed"} = conn) do
    Logger.info("Connecting to server: #{conn.host}:#{conn.port}")

    with {:ok, socket} <- :gen_tcp.connect(conn.host, conn.port, [:binary, active: true]),
         :ok <- :gen_tcp.controlling_process(socket, self()) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        %{conn | socket: socket, state: "connecting"}
        |> Map.update!(:requests, &Map.put(&1, :connect, from))
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

  @impl true
  def handle_call({:close, code, reason}, from, %Connection{state: "open"} = conn) do
    Logger.info("Connection close requested by client: #{code} #{reason}")

    conn =
      %{conn | state: "closing"}
      |> Map.update!(:requests, &Map.put(&1, :close, from))
      |> send_request(:close, code: code, reason: reason)

    {:noreply, conn}
  end

  @impl true
  def handle_info({:tcp, _socket, data}, conn) do
    conn =
      case Message.decode!(data) do
        %Request{} = decoded ->
          handle(conn, decoded)

        %Response{code: %Ok{}} = decoded ->
          handle(conn, decoded)

        decoded ->
          handle_error(conn, decoded)
      end

    {:noreply, conn}
  end

  @impl true
  def handle_info({:heartbeat}, conn) do
    conn = send_request(conn, :heartbeat, sum: 0)

    Process.send_after(self(), {:heartbeat}, conn.heartbeat * 1000)

    {:noreply, conn}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, conn) do
    {:noreply, conn}
  end

  @impl true
  def handle_info({:tcp_error, _socket, _reason}, conn) do
    {:noreply, conn}
  end

  defp handle(conn, %Response{command: %Close{}}) do
    Logger.debug("Connection closed: #{conn.host}:#{conn.port}")

    client = conn.requests.close

    conn =
      %{conn | state: "closed"}
      |> Map.update!(:requests, &%{&1 | close: nil})

    GenServer.reply(client, {:ok, conn})

    conn
  end

  defp handle(%Connection{} = conn, %Request{command: %Close{}} = request) do
    Logger.debug("Connection close requested by server: #{request.data.code} #{request.data.reason}")
    Logger.debug("Connection closed")

    %{conn | state: "closing"}
    |> send_response(:close, correlation_id: request.correlation_id, code: %Response.Code.Ok{})
  end

  defp handle(%Connection{state: "closed"} = conn, _) do
    Logger.error("Message received on a closed connection")

    conn
  end

  defp handle(%Connection{} = conn, %Response{command: %PeerProperties{}} = request) do
    Logger.debug("Exchange successful.")
    Logger.debug("Initiating SASL handshake.")

    %{conn | peer_properties: request.data.peer_properties}
    |> send_request(:sasl_handshake)
  end

  defp handle(%Connection{} = conn, %Response{command: %SaslHandshake{}} = request) do
    Logger.debug("SASL handshake successful. Initiating authentication.")

    %{conn | mechanisms: request.data.mechanisms}
    |> send_request(:sasl_authenticate)
  end

  defp handle(%Connection{} = conn, %Response{command: %SaslAuthenticate{}, data: %{sasl_opaque_data: ""}}) do
    Logger.debug("Authentication successful. Initiating connection tuning.")

    conn
  end

  defp handle(%Connection{} = conn, %Response{command: %SaslAuthenticate{}}) do
    Logger.debug("Authentication successful. Skipping connection tuning.")
    Logger.debug("Opening connection to vhost: \"#{conn.vhost}\"")

    conn
    |> send_request(:open)
    |> Map.put(:state, "opening")
  end

  defp handle(%Connection{} = conn, %Response{command: %Tune{}} = request) do
    Logger.debug("Tunning complete. Starting heartbeat timer.")

    Process.send_after(self(), {:heartbeat}, conn.heartbeat * 1000)

    %{conn | frame_max: request.data.frame_max, heartbeat: request.data.heartbeat}
  end

  defp handle(%Connection{} = conn, %Request{command: %Tune{}} = request) do
    Logger.debug("Tunning data received. Starting heartbeat timer.")
    Logger.debug("Opening connection to vhost: \"#{conn.vhost}\"")

    %{conn | frame_max: request.data.frame_max, heartbeat: request.data.heartbeat}
    |> send_response(:tune, correlation_id: request.correlation_id)
    |> Map.put(:state, "opening")
    |> send_request(:open)
  end

  defp handle(%Connection{} = conn, %Response{command: %Open{}} = request) do
    Logger.debug("Successfully opened connection with vhost: \"#{conn.vhost}\"")

    client = conn.requests.connect

    conn =
      %{conn | state: "open", connection_properties: request.data.connection_properties}
      |> Map.update!(:requests, &%{&1 | connect: nil})

    GenServer.reply(client, {:ok, conn})

    conn
  end

  defp handle(%Connection{} = conn, %Request{command: %Heartbeat{}}) do
    conn
  end

  def handle_error(%Connection{} = conn, %Response{code: code})
      when code in [
             %SaslMechanismNotSupported{},
             %AuthenticationFailure{},
             %SaslError{},
             %SaslChallenge{},
             %SaslAuthenticationFailureLoopback{},
             %VirtualHostAccessFailure{}
           ] do
    Logger.error("Failed to connect to #{conn.host}:#{conn.port}. Reason: #{code}")

    GenServer.reply(conn.requests.connect, {:error, code})

    %{conn | state: "closed"}
    |> Map.update!(:requests, &%{&1 | connect: nil})
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
end
