defmodule RabbitMQStream.Publisher.LifeCycle do
  @moduledoc false
  use GenServer

  # Callbacks
  @impl GenServer
  def init(opts \\ []) do
    reference_name = Keyword.get(opts, :reference_name, Atom.to_string(opts[:publisher_module]))
    connection = Keyword.get(opts, :connection) || raise(":connection is required")
    stream_name = Keyword.get(opts, :stream_name) || raise(":stream_name is required")

    state = %RabbitMQStream.Publisher{
      id: nil,
      sequence: nil,
      stream_name: stream_name,
      connection: connection,
      reference_name: reference_name,
      publisher_module: opts[:publisher_module]
    }

    {:ok, state, {:continue, opts}}
  end

  @impl GenServer
  def handle_continue(opts, state) do
    state = apply(state.publisher_module, :before_start, [opts, state])

    with {:ok, id} <-
           RabbitMQStream.Connection.declare_publisher(state.connection, state.stream_name, state.reference_name),
         {:ok, sequence} <-
           RabbitMQStream.Connection.query_publisher_sequence(state.connection, state.stream_name, state.reference_name) do
      {:noreply, %{state | id: id, sequence: sequence + 1}}
    else
      err ->
        {:stop, err, state}
    end
  end

  @impl GenServer
  def handle_cast({:publish, {message, filter_value}}, %RabbitMQStream.Publisher{} = state) when is_binary(message) do
    :ok = RabbitMQStream.Connection.publish(state.connection, state.id, state.sequence, message, filter_value)

    {:noreply, %{state | sequence: state.sequence + 1}}
  end

  @impl GenServer
  def terminate(_reason, %{id: nil}), do: :ok

  def terminate(_reason, state) do
    RabbitMQStream.Connection.delete_publisher(state.connection, state.id)
    :ok
  end
end
