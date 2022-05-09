defmodule XColony.Connection do
  use GenServer
  require Logger

  alias __MODULE__, as: Connection

  alias XColony.Message.Request

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
    version: 1,
    state: "down",
    correlation: 1,
    requests: []
  ]


  def start_link(default \\ []) when is_list(default) do
    GenServer.start_link(__MODULE__, default)
  end

  def connect(pid) do
    GenServer.call(pid, :connect)
  end

  defp send_command(%Connection{}=conn, command) do
    case command do
      :peer_properties ->
        frame = Request.new!(:peer_properties, conn.version, conn.correlation) |> Request.encode!()
        {:gen_tcp.send(conn.socket, frame), %{conn | correlation:  conn.correlation + 1}}


      :sasl_handshake ->
        frame = Request.new!(:sasl_handshake, conn.version, conn.correlation) |> Request.encode!()
        {:gen_tcp.send(conn.socket, frame), %{conn | correlation:  conn.correlation + 1}}

      :sasl_authenticate ->
        frame = Request.new!(:sasl_authenticate, conn.version, conn.correlation) |> Request.encode!()
        {:gen_tcp.send(conn.socket, frame), %{conn | correlation: conn.correlation + 1}}

    end
  end

  @impl true
  def init(opts \\ []) do
    username = opts[:username] || "guest"
    password = opts[:password] || "guest"
    host = opts[:host] || 'localhost'
    port = opts[:port] || 5552

    with {:ok, socket} <- :gen_tcp.connect(host, port, [:binary]),
      :ok <- :gen_tcp.controlling_process(socket,self()) do
      conn = %Connection{
        host: host,
        port: port,
        username: username,
        password: password,
        state: "started",
        socket: socket,
        correlation: 2,
      }
      with {:ok, conn} <- send_command(conn, :peer_properties) do
      #  {:ok, conn} <- send_command(conn, :sasl_handshake),
      #  {:ok, conn} <- send_command(conn, :sasl_authenticate) do
        {:ok,conn}
       end
    end
  end


  @impl true
  def handle_info({:tcp, _socket, data}, conn) do
    IO.inspect("DATA")
    IO.inspect(data)
    {:noreply, conn}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, conn) do
    IO.inspect("CLOSED")
    {:noreply, conn}
  end

  @impl true
  def handle_info({:tcp_error, _socket, reason}, conn) do
    IO.inspect("ERR")
    IO.inspect(reason)
    {:noreply, conn}
  end
end
