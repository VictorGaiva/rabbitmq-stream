defmodule RabbitMQStream.Connection.Client do
  @moduledoc false

  require Logger

  use GenServer

  alias RabbitMQStream.Message.Request
  alias RabbitMQStream.Connection
  alias RabbitMQStream.Connection.{Handler, Helpers}

  alias RabbitMQStream.Message
  alias RabbitMQStream.Message.{Buffer, Encoder}

  @impl GenServer
  def init(options) do
    {transport, options} = Keyword.pop(options, :transport, :tcp)

    transport =
      case transport do
        :tcp -> RabbitMQStream.Connection.Transport.TCP
        :ssl -> RabbitMQStream.Connection.Transport.SSL
        transport -> transport
      end

    conn = %RabbitMQStream.Connection{options: options, transport: transport}

    if options[:lazy] == true do
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
      conn
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

  def handle_call({command, opts}, from, %Connection{} = conn)
      when command in [:route, :partitions] and is_map_key(conn.server_commands_versions, command) do
    conn =
      conn
      |> Helpers.push_request_tracker(command, from)
      |> send_request(command, opts)

    {:noreply, conn}
  end

  def handle_call({command, _opts}, _from, %Connection{peer_properties: %{"version" => version}} = conn)
      when command in [:route, :partitions] do
    Logger.error("Command #{command} is not supported by the server. Its current informed version is '#{version}'.")

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
      case {opts[:message], conn.server_commands_versions[:publish]} do
        {{_, _, filter_value}, {_, 2}} when is_binary(filter_value) ->
          Logger.error("Publishing a message with a `filter_value` is only supported by RabbitMQ on versions >= 3.13")

          conn

        _ ->
          conn
          |> send_request(:publish, opts ++ [correlation_sum: 0])
      end

    {:noreply, conn}
  end

  def handle_cast({:credit, opts}, %Connection{} = conn) do
    conn =
      conn
      |> send_request(:credit, opts)

    {:noreply, conn}
  end

  def handle_cast({:respond, %Request{} = request, opts}, %Connection{} = conn) do
    conn =
      conn
      |> send_response(request.command, [correlation_id: request.correlation_id] ++ opts)

    {:noreply, conn}
  end

  @impl GenServer
  def handle_info({key, _socket, data}, conn) when key in [:ssl, :tcp] do
    {commands, frames_buffer} =
      data
      |> Buffer.incoming_data(conn.frames_buffer)
      |> Buffer.all_commands()

    conn = %{conn | frames_buffer: frames_buffer}

    # A single frame can have multiple commands, and each might have multiple responses.
    # So we first handle each received command, and only then we 'flush', or send, each
    # command to the socket. This also would allow us to better test the 'handler' logic.
    commands
    |> Enum.reduce(conn, &Handler.handle_message(&2, &1))
    |> flush_commands()
    |> handle_closing()
  end

  def handle_info({key, _socket}, conn) when key in [:tcp_closed, :ssl_closed] do
    if conn.state == :connecting do
      Logger.warning(
        "The connection was closed by the host, after the socket was already open, while running the authentication sequence. This could be caused by the server not having Stream Plugin active"
      )
    end

    %{conn | close_reason: key}
    |> handle_closing()
  end

  def handle_info({key, _socket, reason}, conn) when key in [:tcp_error, :ssl_error] do
    %{conn | close_reason: reason}
    |> handle_closing()
  end

  def handle_info({:heartbeat}, conn) do
    Process.send_after(self(), {:heartbeat}, conn.options[:heartbeat] * 1000)

    conn = send_request(conn, :heartbeat, correlation_sum: 0)

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
    with {:ok, socket} <- conn.transport.connect(conn.options) do
      {:ok, %{conn | socket: socket, state: :connecting}}
    end
  end

  defp handle_closing(%Connection{state: :closing} = conn) do
    for request <- conn.connect_requests do
      GenServer.reply(request, {:error, :closed})
    end

    if is_port(conn.socket) do
      :ok = conn.transport.close(conn.socket)
    end

    for {client, _data} <- Map.values(conn.request_tracker) do
      GenServer.reply(client, {:error, {:closed, conn.close_reason}})
    end

    conn = %{conn | request_tracker: %{}, connect_requests: [], socket: nil, state: :closed, close_reason: nil}

    {:noreply, conn, :hibernate}
  end

  defp handle_closing(conn), do: {:noreply, conn}

  defp send_request(%Connection{} = conn, command, opts \\ []) do
    conn
    |> Helpers.push(:request, command, opts)
    |> flush_commands()
  end

  defp send_response(%Connection{} = conn, command, opts) do
    conn
    |> Helpers.push(:response, command, opts)
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

    :ok = conn.transport.send(conn.socket, frame)

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
    frame =
      conn
      |> Message.new_response(command, opts)
      |> Encoder.encode()

    :ok = conn.transport.send(conn.socket, frame)

    conn
  end
end
