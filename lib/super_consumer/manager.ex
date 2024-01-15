defmodule RabbitMQStream.SuperConsumer.Manager do
  alias RabbitMQStream.SuperConsumer

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts \\ []) do
    state = struct(SuperConsumer, opts)

    {:ok, state, {:continue, :start}}
  end

  @impl true
  def handle_continue(:start, %SuperConsumer{} = state) do
    # If stream exists, fetch its paritions information
    streams = Enum.map(state.partitions, &"#{state.super_stream}-#{&1}")
    {:ok, data} = RabbitMQStream.Connection.query_metadata(state.connection, streams)

    # We create all the missing streams
    for %{code: :stream_does_not_exist, name: name} <- data.streams do
      :ok = RabbitMQStream.Connection.create_stream(state.connection, name)
    end

    for partition <- state.partitions do
      # We want to start each child, but don't really care about its state
      {:ok, _pid} =
        DynamicSupervisor.start_child(
          state.dynamic_supervisor,
          {
            RabbitMQStream.Consumer,
            Keyword.merge(state.consumer_opts,
              name: {:via, Registry, {state.registry, partition}},
              initial_offset: :last,
              connection: state.connection,
              stream_name: "#{state.super_stream}-#{partition}",
              offset_reference: "#{state.super_stream}-#{partition}",
              consumer_module: __MODULE__,
              properties: [
                single_active_consumer: true,
                super_stream: state.super_stream
              ]
            )
          }
        )
    end

    {:noreply, state}
  end

  def handle_update(_, true) do
    {:ok, :last}
  end
end
