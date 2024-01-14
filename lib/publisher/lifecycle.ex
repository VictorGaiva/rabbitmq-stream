defmodule RabbitMQStream.Publisher.Lifecycle do
  @moduledoc false
  use GenServer

  # Callbacks
  @impl GenServer
  def init(opts \\ []) do
    reference_name = Keyword.get(opts, :reference_name, Atom.to_string(opts[:publisher_module]))
    connection = Keyword.get(opts, :connection) || raise(":connection is required")
    stream_name = Keyword.get(opts, :stream_name) || raise(":stream_name is required")

    # An implemented `encode!/1` callback takes precedence over the serializer option
    serializer = Keyword.get(opts, :serializer, opts[:publisher_module])

    state = %RabbitMQStream.Publisher{
      id: nil,
      sequence: nil,
      stream_name: stream_name,
      connection: connection,
      reference_name: reference_name,
      serializer: serializer,
      publisher_module: opts[:publisher_module]
    }

    {:ok, state, {:continue, opts}}
  end

  @impl GenServer
  def handle_continue(opts, state) do
    state = apply(state.publisher_module, :before_start, [opts, state])

    with {:ok, id} <- state.connection.declare_publisher(state.stream_name, state.reference_name),
         {:ok, sequence} <- state.connection.query_publisher_sequence(state.stream_name, state.reference_name) do
      {:noreply, %{state | id: id, sequence: sequence + 1}}
    else
      err ->
        {:stop, err, state}
    end
  end

  @impl GenServer
  def handle_cast({:publish, {message, filter_value}}, %RabbitMQStream.Publisher{} = state) when is_binary(message) do
    :ok = state.connection.publish(state.id, state.sequence, message, filter_value)

    {:noreply, %{state | sequence: state.sequence + 1}}
  end

  @impl GenServer
  def terminate(_reason, %{id: nil}), do: :ok

  def terminate(_reason, state) do
    state.connection.delete_publisher(state.id)
    :ok
  end
end
