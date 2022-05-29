defmodule RabbitMQStream.Publisher do
  @moduledoc """
  A GenServer module that, using a provided connection, declares itself under given `name`,
  and provides an interface for publishing messages on `stream_name` stream.
  It also keeps track of the current publishing id, to avoid message duplication.
  """

  use GenServer

  alias RabbitMQStream.Connection

  alias __MODULE__

  defstruct [
    :publishing_id,
    :reference_name,
    :connection,
    :stream_name,
    :sequence,
    :id
  ]

  def publish(pid, message, sequence \\ nil) when is_binary(message) do
    GenServer.cast(pid, {:publish, message, sequence})
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: args[:name])
  end

  ## Callbacks
  @impl true
  def init(opts) do
    reference_name = opts[:reference_name] || Atom.to_string(__MODULE__)
    connection = opts[:connection]
    stream_name = opts[:stream_name]

    with :ok <- Connection.connect(connection),
         {:ok, id} <- Connection.declare_publisher(connection, stream_name, reference_name),
         {:ok, sequence} <- Connection.query_publisher_sequence(connection, stream_name, reference_name) do
      state = %Publisher{
        id: id,
        stream_name: stream_name,
        connection: connection,
        reference_name: reference_name,
        sequence: sequence + 1
      }

      {:ok, state}
    else
      {:error, :stream_does_not_exist} ->
        with :ok <- Connection.create_stream(connection, stream_name),
             {:ok, id} <- Connection.declare_publisher(connection, stream_name, reference_name),
             {:ok, sequence} <- Connection.query_publisher_sequence(connection, stream_name, reference_name) do
          state = %Publisher{
            id: id,
            stream_name: stream_name,
            connection: connection,
            reference_name: reference_name,
            sequence: sequence + 1
          }

          {:ok, state}
        end
    end
  end

  @impl true
  def handle_call({:get_state}, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_cast({:publish, message, nil}, %Publisher{} = publisher) do
    with :ok <- Connection.connect(publisher.connection) do
      Connection.publish(publisher.connection, publisher.id, publisher.sequence, message)

      {:noreply, %{publisher | sequence: publisher.sequence + 1}}
    else
      _ -> {:reply, {:error, :closed}, publisher}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Connection.delete_publisher(state.connection, state.id)
    :ok
  end

  if Mix.env() == :test do
    def get_state(pid) do
      GenServer.call(pid, {:get_state})
    end
  end
end
