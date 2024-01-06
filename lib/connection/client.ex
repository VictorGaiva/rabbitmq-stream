defmodule RabbitMQStream.Connection.Client do
  @moduledoc false

  require Logger

  use GenServer
  use RabbitMQStream.Connection.Handler

  alias RabbitMQStream.Connection
  alias RabbitMQStream.Connection.Helpers

  alias RabbitMQStream.Message.Buffer

  @impl GenServer
  def init(opts) do
    conn = %RabbitMQStream.Connection{
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

  @impl GenServer
  def handle_call({:connect}, from, %Connection{state: :closed} = conn) do
    Logger.debug("Connecting to server: #{conn.options[:host]}:#{conn.options[:port]}")

    with {:ok, conn} <- connect(conn) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        %{conn | connect_requests: [from | conn.connect_requests]}
        |> send_request(:peer_properties)

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
    {:noreply, %{conn | connect_requests: [from | conn.connect_requests]}}
  end

  # Replies with `:ok` if the connection is already closed. Not sure if this behavior is the best.
  def handle_call({:close, _reason, _code}, _from, %Connection{state: :closed} = conn) do
    {:reply, :ok, conn}
  end

  def handle_call(action, from, %Connection{state: state} = conn) when state != :open do
    {:noreply, %{conn | request_buffer: :queue.in({:call, {action, from}}, conn.request_buffer)}}
  end

  def handle_call({:close, reason, code}, from, %Connection{} = conn) do
    Logger.debug("Connection close requested by client: #{reason} #{code}")

    conn =
      %{conn | state: :closing}
      |> Helpers.push_request_tracker(:close, from)
      |> send_request(:close, reason: reason, code: code)

    {:noreply, conn}
  end

  def handle_call({:subscribe, opts}, from, %Connection{} = conn) do
    subscription_id = conn.subscriber_sequence

    conn =
      conn
      |> Helpers.push_request_tracker(:subscribe, from, {subscription_id, opts[:pid]})
      |> send_request(:subscribe, opts ++ [subscriber_sum: 1, subscription_id: subscription_id])

    {:noreply, conn}
  end

  def handle_call({:unsubscribe, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> Helpers.push_request_tracker(:unsubscribe, from, opts[:subscription_id])
      |> send_request(:unsubscribe, opts)

    {:noreply, conn}
  end

  def handle_call({command, opts}, from, %Connection{} = conn)
      when command in [
             :query_offset,
             :delete_publisher,
             :query_metadata,
             :query_publisher_sequence,
             :delete_stream,
             :create_stream
           ] do
    conn =
      conn
      |> Helpers.push_request_tracker(command, from)
      |> send_request(command, opts)

    {:noreply, conn}
  end

  def handle_call({command, opts}, from, %Connection{peer_properties: %{"version" => version}} = conn)
      when command in [:route, :partitions] and version >= [3, 11] do
    conn =
      conn
      |> Helpers.push_request_tracker(command, from)
      |> send_request(command, opts)

    {:noreply, conn}
  end

  def handle_call({command, _opts}, _from, %Connection{peer_properties: %{"version" => version}} = conn)
      when command in [:route, :partitions] and version <= [3, 11] do
    Logger.error("Command #{command} is not supported by the server version #{version}")

    {:reply, {:error, :unsupported}, conn}
  end

  def handle_call({:declare_publisher, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> Helpers.push_request_tracker(:declare_publisher, from, conn.publisher_sequence)
      |> send_request(:declare_publisher, opts ++ [publisher_sum: 1])

    {:noreply, conn}
  end

  @impl GenServer
  def handle_cast(action, %Connection{state: state} = conn) when state != :open do
    {:noreply, %{conn | request_buffer: :queue.in({:cast, action}, conn.request_buffer)}}
  end

  def handle_cast({:store_offset, opts}, %Connection{} = conn) do
    conn =
      conn
      |> send_request(:store_offset, opts)

    {:noreply, conn}
  end

  def handle_cast({:publish, opts}, %Connection{} = conn) do
    conn =
      conn
      |> send_request(:publish, opts ++ [correlation_sum: 0])

    {:noreply, conn}
  end

  def handle_cast({:credit, opts}, %Connection{} = conn) do
    conn =
      conn
      |> send_request(:credit, opts)

    {:noreply, conn}
  end

  @impl GenServer
  def handle_info({:tcp, _socket, data}, conn) do
    {commands, frames_buffer} =
      data
      |> Buffer.incoming_data(conn.frames_buffer)
      |> Buffer.all_commands()

    conn = %{conn | frames_buffer: frames_buffer}

    # A single frame can have multiple commands, and each might have multiple responses.
    # So we first handle each received command, and only then we 'flush', or send, each
    # command to the socket. This also would allow us to better test the 'handler' logic.
    conn =
      commands
      |> Enum.reduce(conn, &handle_message(&2, &1))
      |> flush_commands()

    cond do
      conn.state == :closed ->
        {:noreply, conn, :hibernate}

      true ->
        {:noreply, conn}
    end
  end

  def handle_info({:tcp_closed, _socket}, conn) do
    if conn.state == :connecting do
      Logger.warning(
        "The connection was closed by the host, after the socket was already open, while running the authentication sequence. This could be caused by the server not having Stream Plugin active"
      )
    end

    conn = handle_closed(conn, :tcp_closed)

    {:noreply, conn, :hibernate}
  end

  def handle_info({:tcp_error, _socket, reason}, conn) do
    conn = handle_closed(conn, reason)

    {:noreply, conn}
  end

  def handle_info({:heartbeat}, conn) do
    conn = send_request(conn, :heartbeat, correlation_sum: 0)

    Process.send_after(self(), {:heartbeat}, conn.options[:heartbeat] * 1000)

    {:noreply, conn}
  end

  def handle_info(:flush_request_buffer, %Connection{state: :closed} = conn) do
    Logger.warning("Connection is closed. Ignoring flush buffer request.")
    {:noreply, conn}
  end

  # I'm not really sure how to test this behavior at the moment.
  def handle_info(:flush_request_buffer, %Connection{} = conn) do
    # There is probably a better way to reprocess the buffer, but I'm not sure how to do at the moment.
    conn =
      :queue.fold(
        fn
          {:call, {action, from}}, conn ->
            {:noreply, conn} = handle_call(action, from, conn)
            conn

          {:cast, action}, conn ->
            {:noreply, conn} = handle_cast(action, conn)
            conn
        end,
        conn,
        conn.request_buffer
      )

    {:noreply, %{conn | request_buffer: :queue.new()}}
  end

  @impl GenServer
  def handle_continue({:connect}, %Connection{state: :closed} = conn) do
    Logger.debug("Connecting to server: #{conn.options[:host]}:#{conn.options[:port]}")

    with {:ok, conn} <- connect(conn) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        conn
        |> send_request(:peer_properties)

      {:noreply, conn}
    else
      _ ->
        Logger.error("Failed to connect to #{conn.options[:host]}:#{conn.options[:port]}")
        {:noreply, conn}
    end
  end

  defp connect(%Connection{} = conn) do
    with {:ok, socket} <-
           :gen_tcp.connect(String.to_charlist(conn.options[:host]), conn.options[:port], [:binary, active: true]),
         :ok <- :gen_tcp.controlling_process(socket, self()) do
      {:ok, %{conn | socket: socket, state: :connecting}}
    end
  end

  defp handle_closed(%Connection{} = conn, reason) do
    for request <- conn.connect_requests do
      GenServer.reply(request, {:error, :closed})
    end

    for {client, _data} <- Map.values(conn.request_tracker) do
      GenServer.reply(client, {:error, reason})
    end

    %{conn | request_tracker: %{}, connect_requests: [], socket: nil, state: :closed}
  end

  defp send_request(%Connection{} = conn, command, opts \\ []) do
    conn
    |> Helpers.push(:request, command, opts)
    |> flush_commands()
  end

  defp flush_commands(%Connection{} = conn) do
    conn =
      :queue.fold(
        fn
          command, conn ->
            send_command(conn, command)
        end,
        conn,
        conn.commands_buffer
      )

    %{conn | commands_buffer: :queue.new()}
  end

  defp send_command(%Connection{} = conn, {:request, command, opts}) do
    {correlation_sum, opts} = Keyword.pop(opts, :correlation_sum, 1)
    {publisher_sum, opts} = Keyword.pop(opts, :publisher_sum, 0)
    {subscriber_sum, opts} = Keyword.pop(opts, :subscriber_sum, 0)

    frame =
      conn
      |> Message.new_request(command, opts)
      |> Encoder.encode()

    :ok = :gen_tcp.send(conn.socket, frame)

    correlation_sequence = conn.correlation_sequence + correlation_sum
    publisher_sequence = conn.publisher_sequence + publisher_sum
    subscriber_sequence = conn.subscriber_sequence + subscriber_sum

    %{
      conn
      | correlation_sequence: correlation_sequence,
        publisher_sequence: publisher_sequence,
        subscriber_sequence: subscriber_sequence
    }
  end

  defp send_command(%Connection{} = conn, {:response, command, opts}) do
    frame = Message.new_response(conn, command, opts) |> Encoder.encode()
    :ok = :gen_tcp.send(conn.socket, frame)

    conn
  end
end
