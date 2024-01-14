defmodule RabbitMQStream.SuperConsumer.Manager do
  defmodule PartitionConsumer do
    use RabbitMQStream.Consumer,
      initial_offset: :last

    def handle_update(_, true) do
      {:ok, :last}
    end
  end

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
    {:ok, data} =
      state.partitions
      |> Enum.map(&"#{state.super_stream}-#{&1}")
      |> state.connection.query_metadata()

    # We create all the missing streams
    for %{code: :stream_does_not_exist, name: name} <- data.streams do
      :ok = state.connection.create_stream(name)
    end

    dbg(state.partitions)

    for partition <- state.partitions do
      dbg(partition)
      # We want to start each child, but don't really care about its state
      {:ok, _pid} =
        DynamicSupervisor.start_child(
          state.dynamic_supervisor,
          {
            PartitionConsumer,
            Keyword.merge(state.consumer_opts,
              name: partition,
              connection: state.connection,
              stream_name: "#{state.super_stream}-#{partition}",
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
end
