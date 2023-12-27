defmodule RabbitMQStream.Publisher do
  @moduledoc """
  `RabbitMQStream.Publisher` allows you to define modules or processes that publish messages to a single stream.

  ## Defining a publisher Module

  A standalone publisher module can be defined with:

      defmodule MyApp.MyPublisher do
        use RabbitMQStream.Publisher,
          stream_name: "my-stream",
          connection: MyApp.MyConnection
      end

  After adding it to your supervision tree, you can publish messages with:

      MyApp.MyPublisher.publish("Hello, world!")

  You can add the publisher to your supervision tree as follows this:

      def start(_, _) do
        children = [
          # ...
          MyApp.MyPublisher
        ]

        opts = # ...
        Supervisor.start_link(children, opts)
      end

  The standalone publisher starts its own `RabbitMQStream.Connection`, declaring itself and fetching its most recent `publishing_id`, and declaring the stream, if it does not exist.

  ## Configuration
  The RabbitMQStream.Publisher accepts the following options:

  * `stream_name` - The name of the stream to publish to. Required.
  * `reference_name` - The string which is used by the server to prevent [Duplicate Message](https://blog.rabbitmq.com/posts/2021/07/rabbitmq-streams-message-deduplication/). Defaults to `__MODULE__.Publisher`.
  * `connection` - The identifier for a `RabbitMQStream.Connection`.

  You can also declare the configuration in your `config.exs`:

      config :rabbitmq_stream, MyApp.MyPublisher,
        stream_name: "my-stream",
        connection: MyApp.MyConnection

  """

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use GenServer
      @opts opts

      def start_link(opts \\ []) do
        opts =
          Application.get_env(:rabbitmq_stream, __MODULE__, [])
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)

        # opts = Keyword.merge(@opts, opts)
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
      end

      def publish(message, sequence \\ nil) when is_binary(message) do
        GenServer.cast(__MODULE__, {:publish, message, sequence})
      end

      def stop() do
        GenServer.stop(__MODULE__)
      end

      ## Callbacks
      @impl true
      def init(opts \\ []) do
        reference_name = Keyword.get(opts, :reference_name, __MODULE__)
        connection = Keyword.get(opts, :connection) || raise(":connection is required")
        stream_name = Keyword.get(opts, :stream_name) || raise(":stream_name is required")

        with :ok <- connection.connect(),
             {:ok, id} <- connection.declare_publisher(stream_name, reference_name),
             {:ok, sequence} <- connection.query_publisher_sequence(stream_name, reference_name) do
          state = %RabbitMQStream.Publisher{
            id: id,
            stream_name: stream_name,
            connection: connection,
            reference_name: reference_name,
            sequence: sequence + 1
          }

          {:ok, state}
        else
          {:error, :stream_does_not_exist} ->
            with :ok <- connection.create_stream(stream_name),
                 {:ok, id} <- connection.declare_publisher(stream_name, reference_name),
                 {:ok, sequence} <- connection.query_publisher_sequence(stream_name, reference_name) do
              state = %RabbitMQStream.Publisher{
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
      def handle_cast({:publish, message, nil}, %RabbitMQStream.Publisher{} = publisher) do
        with :ok <- publisher.connection.connect() do
          publisher.connection.publish(publisher.id, publisher.sequence, message)

          {:noreply, %{publisher | sequence: publisher.sequence + 1}}
        else
          _ -> {:reply, {:error, :closed}, publisher}
        end
      end

      @impl true
      def terminate(_reason, state) do
        state.connection.delete_publisher(state.id)
        :ok
      end

      if Mix.env() == :test do
        def get_state() do
          GenServer.call(__MODULE__, {:get_state})
        end
      end
    end
  end

  defstruct [
    :publishing_id,
    :reference_name,
    :connection,
    :stream_name,
    :sequence,
    :id
  ]
end
