defmodule RabbitMQStream.Client.Lifecycle do
  use GenServer
  require Logger
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

  # 1. Should consumer's commands other than 'subscribe' be sent to control or its own connection?
  #

  @impl true
  def handle_call({:subscribe, opts} = args, _from, %Client{} = conn) do
    stream = Keyword.fetch!(opts, :stream_name)
    consumer_pid = Keyword.fetch!(opts, :pid)

    # We start a new conneciton on every 'subscribe' request.
    case new_leader_connection(conn, stream) do
      {:ok, broker_pid} ->
        # Listen to each process's lifecycle
        consumer_ref = Process.monitor(consumer_pid)
        brooker_ref = Process.monitor(broker_pid)

        # Adds it to the subscriptions list
        subscriptions =
          conn.subscriptions
          # And we add both so we can get each other
          |> Map.put(consumer_ref, {:consumer, brooker_ref, {consumer_pid, broker_pid, args}})
          |> Map.put(brooker_ref, {:brooker, consumer_ref, {consumer_pid, broker_pid, args}})

        # Forward the request to the new connection
        {:reply, GenServer.call(broker_pid, args), %{conn | subscriptions: subscriptions}}

      reply ->
        {:reply, reply, conn}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %Client{} = conn) do
    case Map.get(conn.subscriptions, ref) do
      {type, other_ref, {client_pid, broker_pid, args}} ->
        case type do
          # If the process that has shut down is Consumer process
          :consumer ->
            # We should shut down the connection as it is not needed anymore
            :ok = GenServer.stop(broker_pid, :normal, 1000)

          # If it was the broker that shutdown
          :brooker ->
            # We tell the client process that the connection has exited, so it can handle
            # it the way it sees fit. Meaning that a Consumer might re-run the offset-fetching +
            # subscribing flow, while a publisher has to (TODO!...)
            case args do
              {:subscribe, opts} ->
                # We forward the 'opts' so that the user's process can use it if needed.
                send(client_pid, {:resubscribe, opts})

              _ ->
                Logger.info("TODO")
            end

            :ok
        end

        # We always do this cleanup as the 'user' process is responsible for any 'init' or 're-init'
        # flow.
        subscriptions = Map.drop(conn.subscriptions, [ref, other_ref])

        {:noreply, %{conn | subscriptions: subscriptions}}

      nil ->
        Logger.warning("Received :DOWN for #{ref}, but it was not found in 'subscriptions'")
        {:noreply, conn}
    end
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
