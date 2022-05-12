defmodule RabbitStream.Connection do
  use GenServer
  require Logger

  alias __MODULE__, as: Connection

  alias RabbitStream.Message
  alias RabbitStream.Message.{Request,Response}

  alias RabbitStream.Message.Command.Code.{
    SaslHandshake,
    PeerProperties,
    SaslAuthenticate,
    Tune,
    Open,
    Heartbeat
  }

  alias RabbitStream.Message.Data.{
    TuneData,
    PeerPropertiesData,
    SaslHandshakeData,
    OpenData
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
    GenServer.call(pid, :connect)
  end

  @impl true
  def init(opts \\ []) do
    username = opts[:username] || "guest"
    password = opts[:password] || "guest"
    host = opts[:host] || 'localhost'
    port = opts[:port] || 5552
    vhost = opts[:vhost] || "dev"

    with {:ok, socket} <- :gen_tcp.connect(host, port, [:binary, active: true]),
      :ok <- :gen_tcp.controlling_process(socket,self()) do
      conn = %Connection{
        host: host,
        port: port,
        vhost: vhost,
        username: username,
        password: password,
        socket: socket,
        correlation: 2,
      }
      {:ok, send_request(conn, :peer_properties)}
    end
  end


  defp send_request(%Connection{}=conn, command, sum \\ 1) do
    case command do
      :peer_properties ->
        frame = Request.new_encoded!(conn, :peer_properties)
        :ok = :gen_tcp.send(conn.socket, frame)

      :sasl_handshake ->
        frame = Request.new_encoded!(conn, :sasl_handshake)
        :ok = :gen_tcp.send(conn.socket, frame)

      :sasl_authenticate ->
        frame = Request.new_encoded!(conn, :sasl_authenticate)
        :ok = :gen_tcp.send(conn.socket, frame)

      :tune ->
        frame = Request.new_encoded!(conn, :tune)
        :ok = :gen_tcp.send(conn.socket, frame)

      :open ->
        frame = Request.new_encoded!(conn, :open)
        :ok = :gen_tcp.send(conn.socket, frame)

      :heartbeat->
        frame = Request.new_encoded!(conn, :heartbeat)
        :ok = :gen_tcp.send(conn.socket, frame)

    end
    %{conn | correlation: conn.correlation + sum}
  end

  defp send_response(%Connection{}=conn, command) do
    case command do
      {:tune, _} ->
        frame = Response.new_encoded!(conn, command)
        :ok = :gen_tcp.send(conn.socket, frame)

      {:heartbeat, _} ->
        frame = Response.new_encoded!(conn, command)
        :ok = :gen_tcp.send(conn.socket, frame)

    end

    conn
  end

  @impl true
  def handle_info({:tcp, _socket, data}, conn) do
    conn = case Message.decode!(data) do
      %Response{command: %PeerProperties{}, data: %PeerPropertiesData{}=data} ->
        %{conn | peer_properties: data.peer_properties}
        |> send_request(:sasl_handshake)

      %Response{command: %SaslHandshake{}, data: %SaslHandshakeData{} = data} ->
        %{conn | mechanisms: data.mechanisms}
        |> send_request(:sasl_authenticate)

      %Response{command: %SaslAuthenticate{}, response_code: %Response.Code.Ok{}}->
        %{conn | state: "tunning"}

      %Response{command: %Tune{}, data: %TuneData{} = data} ->
        Process.send_after(self(), {:heartbeat}, conn.heartbeat * 1000)
        %{conn | frame_max: data.frame_max, heartbeat: data.heartbeat}

      %Response{command: %Open{}, data: %OpenData{} = data} ->
        %{conn | connection_properties: data.connection_properties, state: "open"}

      %Request{command: %Tune{}, data: %TuneData{} = data, correlation_id: correlation} ->
        %{conn | frame_max: data.frame_max, heartbeat: data.heartbeat}
        |> send_response({:tune, correlation})
        |> send_request(:open)

      %Request{command: %Heartbeat{}} ->
        conn

    end

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

  @impl true
  def handle_info({:heartbeat}, conn) do
    Process.send_after(self(), {:heartbeat}, conn.heartbeat * 1000)

    conn = send_request(conn, :heartbeat, 0)

    {:noreply, conn}
  end

end
