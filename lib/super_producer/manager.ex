defmodule RabbitMQStream.SuperProducer.Manager do
  alias RabbitMQStream.SuperProducer

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts \\ []) do
    state = struct(SuperProducer, opts)

    {:ok, state, {:continue, :start}}
  end

  @impl true
  def handle_continue(:start, %SuperProducer{} = state) do
    for partition <- 0..(state.partitions - 1) do
      {:ok, _pid} =
        DynamicSupervisor.start_child(
          state.dynamic_supervisor,
          {
            RabbitMQStream.Producer,
            Keyword.merge(state.producer_opts,
              name: {:via, Registry, {state.registry, partition}},
              stream_name: "#{state.super_stream}-#{partition}",
              producer_module: state.producer_module
            )
          }
        )
    end

    {:noreply, state, :hibernate}
  end
end
