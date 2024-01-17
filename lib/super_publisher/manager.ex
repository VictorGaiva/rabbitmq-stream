defmodule RabbitMQStream.SuperPublisher.Manager do
  alias RabbitMQStream.SuperPublisher

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts \\ []) do
    state = struct(SuperPublisher, opts)

    {:ok, state, {:continue, :start}}
  end

  @impl true
  def handle_continue(:start, %SuperPublisher{} = state) do
    for partition <- 0..(state.partitions - 1) do
      {:ok, _pid} =
        DynamicSupervisor.start_child(
          state.dynamic_supervisor,
          {
            RabbitMQStream.Publisher,
            Keyword.merge(state.publisher_opts,
              name: {:via, Registry, {state.registry, partition}},
              stream_name: "#{state.super_stream}-#{partition}",
              publisher_module: state.publisher_module
            )
          }
        )
    end

    {:noreply, state, :hibernate}
  end
end
