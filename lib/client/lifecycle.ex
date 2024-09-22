defmodule RabbitMQStream.Client.Lifecycle do
  use GenServer
  alias RabbitMQStream.Client

  @moduledoc """
  This module defines the lifecycle of the RabbitMQStream.Client.

  It is responsible for setting up the connections and routing requests to one or more RabbitMQ
  servers within a cluster. It uses the protocol's nodes disovery mechanisms, mainly `query_metadata`
  to resolve the leader of each stream.

  ### Subscription

  A subscribe request is handled by creating a new `RabbitMQStream.Connection` process, being
  responsible for only that subscriber. If the subscriber dies, the connection is also killed.

  - What should happen if the connection itself dies?

  """

  @impl true
  def init(opts) do
    {client_opts, connection_opts} = Keyword.split(opts, [:auto_discovery, :name, :max_retries, :proxied?, :scope])

    # We specifically want to ignore the 'lazy' option when setting up a child connection
    connection_opts = Keyword.drop(connection_opts, [:lazy])

    client_opts = Keyword.put(client_opts, :connection_opts, connection_opts)

    conn =
      struct(
        RabbitMQStream.Client,
        Keyword.put(client_opts, :opts, client_opts)
      )

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
    subscriber_pid = Keyword.fetch!(opts, :pid)

    # We start a new conneciton on every 'subscribe' request.
    case new_leader_connection(conn, stream) do
      {:ok, broker_conn} ->
        # Listens to the subscriber's lifecycle
        ref = Process.monitor(subscriber_pid)

        # Adds it to the subscriptions list
        subscriptions = Map.put(conn.subscriptions, ref, broker_conn)

        # Forward the request to the new connection
        {:reply, GenServer.call(broker_conn, args), %{conn | subscriptions: subscriptions}}

      reply ->
        {:reply, reply, conn}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %Client{} = conn) do
    dbg(conn)
    # Tells the connection to stop
    brooken_conn = Map.get(conn.subscriptions, ref)

    :ok = GenServer.stop(brooken_conn, :normal, 1000)

    # Remove the subscription from the list
    subscriptions = Map.delete(conn.subscriptions, ref)

    {:noreply, %{conn | subscriptions: subscriptions}}
  end

  # Creates a new connection to the leader of the stream
  defp new_leader_connection(%Client{} = conn, stream, _attempts \\ nil) do
    # Query metadata to identify brokers
    case RabbitMQStream.Connection.query_metadata(conn.control, [stream]) do
      {:ok, %{streams: [%{code: :ok, name: ^stream, leader: leader}]} = metadata} ->
        %{host: host, port: port} = Enum.find(metadata.brokers, &(&1.reference == leader))

        # Start a new connection to the leader
        {:ok, broker_conn} =
          RabbitMQStream.Connection.start_link(
            Keyword.merge(
              conn.opts,
              host: host,
              port: port
            )
          )

        # Ensure the connection is up
        :ok = RabbitMQStream.Connection.connect(broker_conn)

        {:ok, broker_conn}

      _ ->
        {:error, :no_broker_available}
    end
  end
end
