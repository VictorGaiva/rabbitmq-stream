defmodule RabbitMQStream.Client.Lifecycle do
  use GenServer
  alias RabbitMQStream.Client

  @impl true
  def init(opts) do
    {client_opts, connection_opts} = Keyword.split(opts, [:auto_discovery])

    # We specifically want to ignore the 'lazy' option when setting up a child connection
    connection_opts = Keyword.drop(connection_opts, [:lazy])

    conn = %RabbitMQStream.Client{opts: Keyword.put(client_opts, :connection_opts, connection_opts)}

    {:ok, conn, {:continue, :setup}}
  end

  @impl true
  def handle_continue(:setup, conn) do
    connection_opts = Keyword.get(conn.opts, :connection_opts, [])
    # First we start the control connection with the provided options as default
    {:ok, control} = RabbitMQStream.Connection.start_link(connection_opts)

    # Ensure it is connected
    :ok = RabbitMQStream.Connection.connect(control)

    conn =
      conn
      |> Map.put(:control, control)
      |> Map.put(:status, :open)

    {:noreply, conn}
  end

  @impl true
  def handle_call({:subscribe, opts} = args, _from, %Client{} = conn) do
    stream = Keyword.fetch!(opts, :stream_name)
    reference = Map.get(conn.streams, stream)

    if reference == nil do
      # Query metadata to identify brokers
      case RabbitMQStream.Connection.query_metadata(conn.control, [stream]) do
        {:ok, %{streams: [%{code: :ok, name: ^stream, leader: leader}]} = metadata} ->
          case Map.get(conn.brokers, leader) do
            nil ->
              broker = Enum.find(metadata.brokers, &(&1.reference == leader))
              # If the leader is present in the 'streams', it should be present on 'brokers' as well
              # If not, raise
              unless broker do
                raise "Leader not found in the list of brokers"
              end

              # Start a new connection to the leader
              {:ok, broker_conn} =
                RabbitMQStream.Connection.start_link(
                  Keyword.merge(
                    conn.opts,
                    host: broker.host,
                    port: broker.port
                  )
                )

              # Wait for the connection to be established
              :ok = RabbitMQStream.Connection.connect(broker_conn)

              # Fill up the broker state with the new connection
              {:ok, ref} = RabbitMQStream.Connection.monitor(broker_conn)

              brokers =
                Map.update(
                  conn.brokers,
                  leader,
                  %Client.BrokerState{data: broker, connections: %{ref => broker_conn}},
                  &%{&1 | connections: Map.put(&1.connections, ref, broker_conn)}
                )

              streams = Map.put(conn.streams, stream, leader)
              connections = Map.put(conn.connections, ref, leader)

              conn = %{conn | brokers: brokers, streams: streams, connections: connections}

              # Forward the request to the new connection
              {:reply, GenServer.call(broker_conn, args), conn}

            # TODO: Get the broker with the highest priority
            %Client.BrokerState{connections: connections} ->
              # Get any connection
              {_ref, broker_conn} = Enum.random(connections)

              # Forward the request to the existing connection
              {:reply, GenServer.call(broker_conn, args), conn}
          end

        _ ->
          {:reply, {:error, :no_broker_available}, conn}
      end
    else
      {:ok, %Client.BrokerState{connections: connections}} = Map.fetch(conn.brokers, reference)

      {_ref, broker_conn} = Enum.random(connections)

      # Forward the request to the existing connection
      {:reply, GenServer.call(broker_conn, args), conn}
    end
  end

  @impl true
  # O que fazer com o 'ref' do monitor?
  def handle_info({:DOWN, _ref, :process, pid, reason}, conn) do
    dbg({pid, reason})
    {:noreply, conn}
  end

  def handle_info({:rabbitmq_stream, ref, :connection, {:transition, from, to}, _pid, _state}, conn) do
    dbg({ref, from, to})
    {:noreply, conn}
  end
end
