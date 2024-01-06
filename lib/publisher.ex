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


  ## Setting up

  You can optionally define a `before_start/2` callback to perform setup logic, such as creating the stream, if it doesn't yet exists.

      defmodule MyApp.MyPublisher do
        use RabbitMQStream.Publisher,
          stream_name: "my-stream",
          connection: MyApp.MyConnection

        @impl true
        def before_start(_opts, state) do
          # Create the stream
          state.connection.create_stream(state.stream_name)

          state
        end
      end

  ### Configuration

    You can configure each Publisher with:

        config :rabbitmq_stream, MyApp.MyPublisher,
          stream_name: "my-stream",
          connection: MyApp.MyConnection

    And also you can override the defaults of all publishers with:

          config :rabbitmq_stream, :defaults,
            publishers: [
              connection: MyApp.MyConnection,
              # ...
            ]
            serializer: Jason

    Globally configuring all publishers ignores the following options:

      * `:stream_name`
      * `:reference_name`

  ## Serialization

  You can optionally define a callback to encode the message before publishing it.
  It expects that the module you pass implements the `encode!/1` callback.

  You can define it globally:

        config :rabbitmq_stream, :defaults,
          serializer: Jason

  Or you can pass it as an option to the `use` macro:

        defmodule MyApp.MyPublisher do
          use RabbitMQStream.Publisher,
            serializer: Jason
        end

  You can also define it in the module itself:

        defmodule MyApp.MyPublisher do
          use RabbitMQStream.Publisher,
            serializer: __MODULE__

          @impl true
          def encode!(message) do
            Jason.encode!(message)
          end
        end
  """

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use GenServer
      @opts opts

      @behaviour RabbitMQStream.Publisher

      def start_link(opts \\ []) do
        opts =
          Application.get_env(:rabbitmq_stream, :defaults, [])
          |> Keyword.get(:publishers, [])
          |> Keyword.drop([:stream_name, :reference_name])
          |> Keyword.merge(Application.get_env(:rabbitmq_stream, :defaults, []) |> Keyword.take([:serializer]))
          |> Keyword.merge(Application.get_env(:rabbitmq_stream, __MODULE__, []))
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)

        # opts = Keyword.merge(@opts, opts)
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
      end

      def publish(message) do
        GenServer.cast(__MODULE__, {:publish, message})
      end

      def stop() do
        GenServer.stop(__MODULE__)
      end

      ## Callbacks
      @impl true
      def init(opts \\ []) do
        reference_name = Keyword.get(opts, :reference_name, Atom.to_string(__MODULE__))
        connection = Keyword.get(opts, :connection) || raise(":connection is required")
        stream_name = Keyword.get(opts, :stream_name) || raise(":stream_name is required")

        # An implemented `encode/1` callback takes precedence over the encoder option
        encoder = Keyword.get(opts, :encoder, __MODULE__)

        state = %RabbitMQStream.Publisher{
          id: nil,
          sequence: nil,
          stream_name: stream_name,
          connection: connection,
          reference_name: reference_name,
          encoder: encoder
        }

        {:ok, state, {:continue, opts}}
      end

      @impl true
      def handle_continue(opts, state) do
        state = apply(__MODULE__, :before_start, [opts, state])

        with {:ok, id} <- state.connection.declare_publisher(state.stream_name, state.reference_name),
             {:ok, sequence} <- state.connection.query_publisher_sequence(state.stream_name, state.reference_name) do
          {:noreply, %{state | id: id, sequence: sequence + 1}}
        else
          err ->
            {:stop, err, state}
        end
      end

      @impl true
      def handle_cast({:publish, message}, %RabbitMQStream.Publisher{} = publisher) do
        filter_value = apply(__MODULE__, :filter_value, [message])

        message = publisher.encoder.encode!(message)

        publisher.connection.publish(publisher.id, publisher.sequence, message, filter_value)

        {:noreply, %{publisher | sequence: publisher.sequence + 1}}
      end

      @impl true
      def terminate(_reason, %{id: nil}), do: :ok

      def terminate(_reason, state) do
        state.connection.delete_publisher(state.id)
        :ok
      end

      def before_start(_opts, state), do: state
      def filter_value(_), do: nil
      def encode!(message), do: message

      defoverridable RabbitMQStream.Publisher
    end
  end

  defstruct [
    :publishing_id,
    :reference_name,
    :connection,
    :stream_name,
    :sequence,
    :encoder,
    :id
  ]

  @type t :: %__MODULE__{
          publishing_id: String.t(),
          reference_name: String.t(),
          connection: RabbitMQStream.Connection.t(),
          stream_name: String.t(),
          sequence: non_neg_integer() | nil,
          encoder: (term() -> String.t()) | nil,
          id: String.t() | nil
        }

  @type options :: [option()]

  @type option ::
          {:stream_name, String.t()}
          | {:reference_name, String.t()}
          | {:connection, RabbitMQStream.Connection.t()}
  @optional_callbacks [before_start: 2, filter_value: 1, encode!: 1]

  @doc """
    Optional callback that is called after the process has started, but before the
    publisher has declared itself and fetched its most recent `publishing_id`.

    This is usefull for setup logic, such as creating the Stream, if it doesn't yet exists.
  """
  @callback before_start(options(), t()) :: t()

  @callback filter_value(term()) :: String.t()

  @callback encode!(term()) :: String.t()
end
