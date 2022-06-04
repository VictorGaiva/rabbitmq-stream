defmodule RabbitMQStream.Publisher do
  @moduledoc """
  `RabbitMQStream.Publisher` allows you to define modules or processes that publish messages to a single stream.

  ## Defining a publisher Module

  A publisher module can be defined in your applcation as follows:

      defmodule MyApp.MyPublisher do
        use RabbitMQStream.Publisher,
          stream_name: "my-stream"
      end

  This defined a `Supervisor` process which controls a `RabbitMQStream.Publisher` and a `RabbitMQStream.Connection`.

  You can add it to the supervision tree of your application as follows:

      defmodule MyApp do
        use Application

        def start(_, _) do
          children = [
            # ...
            MyApp.MyPublisher
          ]

          opts = [strategy: :one_for_one, name: MyApp.Supervisor]
          Supervisor.start_link(children, opts)
        end
      end


  When a publisher module starts, it creates a `RabbitMQStream.Connection`, and starts its connection with the RabbitMQ server.
  It then declares the Publisher under the specified `stream_name`, fetching the most recent `publishing_id`. It also creates the
  stream in the RabbitMQ server if it doesn't exist.


  ## Dynamically starting publishers

  """

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use Supervisor
      @stream_name Keyword.get(opts, :stream_name) || raise("Stream Name is required")
      @connection Keyword.get(opts, :connection) || []
      @reference_name Keyword.get(opts, :reference_name) || __MODULE__.Publisher |> Atom.to_string()

      def start_link(init_arg) do
        Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
      end

      @impl true
      def init(_) do
        children = [
          {RabbitMQStream.Connection, @connection ++ [name: __MODULE__.Connection]},
          {RabbitMQStream.Publisher,
           [
             connection: __MODULE__.Connection,
             reference_name: @reference_name,
             stream_name: @stream_name,
             name: __MODULE__.Publisher
           ]}
        ]

        Supervisor.init(children, strategy: :one_for_all)
      end

      def publish(message, opts \\ nil) do
        RabbitMQStream.Publisher.publish(__MODULE__.Publisher, message, opts)
      end

      if Mix.env() == :test do
        def get_publisher_state() do
          RabbitMQStream.Publisher.get_state(__MODULE__.Publisher)
        end
      end
    end
  end

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
    reference_name = opts[:reference_name]
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
