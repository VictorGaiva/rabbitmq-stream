defmodule RabbitMQStream.SuperProducer.Manager do
  @moduledoc false
  alias RabbitMQStream.SuperProducer
  alias RabbitMQStream.SuperStream.Helpers
  require Logger

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl true
  def init(opts \\ []) do
    state = struct(SuperProducer, opts)

    {:ok, state, {:continue, :start}}
  end

  @impl true
  def handle_continue(:start, %SuperProducer{} = state) do
    partitions = Helpers.get_partitions(state.connection, state.super_stream, state.partitions)

    for partition <- partitions do
      {:ok, _pid} =
        DynamicSupervisor.start_child(
          state.dynamic_supervisor,
          {
            RabbitMQStream.Producer,
            Keyword.merge(state.producer_opts,
              name: {:via, Registry, {state.registry, partition}},
              stream_name: partition,
              producer_module: state.producer_module
            )
          }
        )
    end

    routes =
      if !RabbitMQStream.Connection.supports?(state.connection, :route) do
        for partition <- 0..(state.partitions - 1), into: %{} do
          {"#{partition}", "#{state.super_stream}-#{partition}"}
        end
      else
        %{}
      end

    {:noreply, %{state | routes: routes}}
  end

  @impl true
  def handle_cast({:publish, message, filter, routing_key}, state) do
    case Map.get(state.routes, routing_key) || get_partitions(state, routing_key) do
      [] ->
        Logger.error("Failed to publish message to SuperStream. No routes found for routing_key: #{routing_key}")
        {:noreply, state}

      partitions ->
        for partition <- partitions do
          GenServer.cast(
            {:via, Registry, {state.registry, partition}},
            {:publish, {message, filter}}
          )
        end

        {:noreply, %{state | routes: Map.put(state.routes, "#{routing_key}", partitions)}}
    end
  end

  defp get_partitions(%RabbitMQStream.SuperProducer{} = state, routing_key) do
    case RabbitMQStream.Connection.route(state.connection, routing_key, state.super_stream) do
      {:ok, data} ->
        data.streams

      _ ->
        []
    end
  end
end
