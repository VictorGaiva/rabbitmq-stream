defmodule RabbitStream.Connection do
  use GenServer
  require Logger

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

  alias RabbitStream.Message.Data.{
    TuneData,
    PeerPropertiesData,
    SaslHandshakeData,
    OpenData,
    SaslAuthenticateData,
    CloseData
  }

  alias __MODULE__, as: Connection

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
    state: "down",
    correlation: 1,
    peer_properties: [],
    connection_properties: [],
    mechanisms: [],
    requests: []
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
  def handle_call({:connect}, _from, conn) do
    Logger.info("Connecting to server: #{conn.host}:#{conn.port}")

    with {:ok, socket} <- :gen_tcp.connect(conn.host, conn.port, [:binary, active: true]),
         :ok <- :gen_tcp.controlling_process(socket, self()) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn = %{conn | socket: socket} |> send_request(:peer_properties)

      {:reply, :ok, conn}
    else
      err ->
        Logger.error("Failed to connect to #{conn.host}:#{conn.port}")
        {:reply, {:error, err}, conn}
    end
  end

  @impl true
  def handle_call({:close, code, reason}, _from, %Connection{state: "open"} = conn) do
    Logger.info("Connection close requested by client: #{code} #{reason}")

    conn =
      %{conn | state: "closing"}
      |> send_request(:close, code: code, reason: reason)

    {:reply, :ok, conn}
  end

  @impl true
  def handle_info({:tcp, _socket, data}, conn) do
    conn = handle(conn, Message.decode!(data))

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

    %{conn | state: "closed"}
  end

  defp handle(conn, %Request{
         command: %Close{},
         data: %CloseData{} = data,
         correlation_id: correlation_id
       }) do
    Logger.debug("Connection close requested by server: #{data.code} #{data.reason}")
    Logger.debug("Connection closed")

    %{conn | state: "closing"}
    |> send_response(:close, correlation_id: correlation_id, code: %Response.Code.Ok{})
  end

  defp handle(%{state: "closed"} = conn, _) do
    Logger.warn("Message received on closed connection")

    conn
  end

  defp handle(conn, %Response{command: %PeerProperties{}, data: %PeerPropertiesData{} = data}) do
    Logger.debug("Exchange successful.")
    Logger.debug("Initiating SASL handshake.")

    %{conn | peer_properties: data.peer_properties}
    |> send_request(:sasl_handshake)
  end

  defp handle(conn, %Response{command: %SaslHandshake{}, data: %SaslHandshakeData{} = data}) do
    Logger.debug("SASL handshake successful. Initiating authentication.")

    %{conn | state: "authenticating", mechanisms: data.mechanisms}
    |> send_request(:sasl_authenticate)
  end

  defp handle(conn, %Response{
         command: %SaslAuthenticate{},
         data: %SaslAuthenticateData{sasl_opaque_data: ""}
       }) do
    Logger.debug("Authentication successful. Initiating connection tuning.")

    %{conn | state: "waiting-tune"}
  end

  defp handle(conn, %Response{command: %SaslAuthenticate{}}) do
    Logger.debug("Authentication successful. Skipping connection tuning.")
    Logger.debug("Opening connection to vhost: \"#{conn.vhost}\"")

    %{conn | state: "auhenticated"}
    |> send_request(:open)
    |> Map.put(:state, "opening")
  end

  defp handle(conn, %Response{command: %Tune{}, data: %TuneData{} = data}) do
    Logger.debug("Tunning complete. Starting heartbeat timer.")

    Process.send_after(self(), {:heartbeat}, conn.heartbeat * 1000)

    %{conn | state: "tuned", frame_max: data.frame_max, heartbeat: data.heartbeat}
  end

  defp handle(conn, %Request{
         command: %Tune{},
         data: %TuneData{} = data,
         correlation_id: correlation_id
       }) do
    Logger.debug("Tunning data received. Starting heartbeat timer.")
    Logger.debug("Opening connection to vhost: \"#{conn.vhost}\"")

    %{conn | state: "tuned", frame_max: data.frame_max, heartbeat: data.heartbeat}
    |> send_response(:tune, correlation_id: correlation_id)
    |> Map.put(:state, "opening")
    |> send_request(:open)
  end

  defp handle(conn, %Response{command: %Open{}, data: %OpenData{} = data}) do
    Logger.debug("Successfully opened connection with vhost: \"#{conn.vhost}\"")

    %{conn | state: "open", connection_properties: data.connection_properties}
  end

  defp handle(conn, %Request{command: %Heartbeat{}}) do
    conn
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
