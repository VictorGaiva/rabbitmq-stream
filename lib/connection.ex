defmodule XColony.Connection do
  use GenServer
  require Logger

  alias __MODULE__, as: Connection

  alias XColony.Message.{Request,Response}
  alias XColony.Message.Command.Code.{
    SaslHandshake,
    PeerProperties,
    SaslAuthenticate,
    Tune
  }

  alias XColony.Message.Response.{
    TuneData,
    PeerPropertiesData,
    SaslHandshakeData
  }

  @type t :: %__MODULE__{
    host: String.t(),
    port: integer(),
    username: String.t(),
    password: String.t(),
    socket: :socket.type()
  }

  defstruct [
    :host,
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

    with {:ok, socket} <- :gen_tcp.connect(host, port, [:binary, active: true]),
      :ok <- :gen_tcp.controlling_process(socket,self()) do
      conn = %Connection{
        host: host,
        port: port,
        username: username,
        password: password,
        state: "peer_properties",
        socket: socket,
        correlation: 2,
      }
      {:ok, send_command(conn, :peer_properties)}
    end
  end


  defp send_command(%Connection{}=conn, command) do
    case command do
      :peer_properties ->
        frame = Request.new!(:peer_properties, conn) |> Request.encode!()
        :ok = :gen_tcp.send(conn.socket, frame)

      :sasl_handshake ->
        frame = Request.new!(:sasl_handshake, conn) |> Request.encode!()
        :ok = :gen_tcp.send(conn.socket, frame)

      :sasl_authenticate ->
        frame = Request.new!(:sasl_authenticate, conn) |> Request.encode!()
        :ok = :gen_tcp.send(conn.socket, frame)

      :tune ->
        frame = Request.new!(:tune, conn) |> Request.encode!()
        :ok = :gen_tcp.send(conn.socket, frame)

    end
    %{conn | correlation: conn.correlation + 1}
  end

  @impl true
  def handle_info({:tcp, _socket, data}, conn) do

    conn = case Response.decode!(data) do
      %Response{command: %PeerProperties{}, data: %PeerPropertiesData{} = data} ->
        conn = %{conn | peer_properties: data.peer_properties}
        send_command(conn, :sasl_handshake)

      %Response{command: %SaslHandshake{}, data: %SaslHandshakeData{}} ->
        send_command(conn, :sasl_authenticate)

      %Response{command: %SaslAuthenticate{}} ->
        %{conn | state: "up"}

      %Response{command: %Tune{}, data: %TuneData{} = data} ->
        conn = %{conn | frame_max: data.frame_max, heartbeat: data.heartbeat}

        send_command(conn, :tune)

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

end
