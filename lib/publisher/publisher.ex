defmodule RabbitMQStream.Publisher do
  @moduledoc """
  `RabbitMQStream.Publisher` allows you to define modules or processes that publish messages to a single stream.

  # Defining a publisher Module

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

  # Configuration
  The RabbitMQStream.Publisher accepts the following options:

  * `:stream_name` - The name of the stream to publish to. Required.
  * `:reference_name` - The string which is used by the server to prevent [Duplicate Message](https://blog.rabbitmq.com/posts/2021/07/rabbitmq-streams-message-deduplication/). Defaults to `__MODULE__.Publisher`.
  * `:connection` - The identifier for a `RabbitMQStream.Connection`.
  * `:serializer` - The module to use to decode the message. Defaults to `nil`, which means no encoding is done.

  You can also declare the configuration in your `config.exs`:

      config :rabbitmq_stream, MyApp.MyPublisher,
        stream_name: "my-stream",
        connection: MyApp.MyConnection


  # Setting up

  You can optionally define a `before_start/2` callback to perform setup logic, such as creating the stream, if it doesn't yet exists.

      defmodule MyApp.MyPublisher do
        use RabbitMQStream.Publisher,
          stream_name: "my-stream",
          connection: MyApp.MyConnection

        @impl true
        def before_start(_opts, state) do
          # Create the stream
          RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

          state
        end
      end

  # Configuration

    You can configure each Publisher with:

        config :rabbitmq_stream, MyApp.MyPublisher,
          stream_name: "my-stream",
          connection: MyApp.MyConnection

    And also you can override the defaults of all publishers with:

          config :rabbitmq_stream, :defaults,
            publisher: [
              connection: MyApp.MyConnection,
              # ...
            ]
            serializer: Jason

    Globally configuring all publishers ignores the following options:

      * `:stream_name`
      * `:reference_name`

  """

  defmacro __using__(opts) do
    defaults = Application.get_env(:rabbitmq_stream, :defaults, [])
    # defaults = Application.compile_env(:rabbitmq_stream, :defaults, [])

    serializer = Keyword.get(opts, :serializer, Keyword.get(defaults, :serializer))

    quote location: :keep do
      @opts unquote(opts)

      @behaviour RabbitMQStream.Publisher

      def start_link(opts \\ []) do
        unless !Keyword.has_key?(opts, :serializer) do
          raise "You can only pass `:serializer` option to compile-time options."
        end

        opts =
          Application.get_env(:rabbitmq_stream, :defaults, [])
          |> Keyword.get(:publisher, [])
          |> Keyword.drop([:stream_name, :reference_name])
          |> Keyword.merge(Application.get_env(:rabbitmq_stream, __MODULE__, []))
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)
          |> Keyword.put(:publisher_module, __MODULE__)

        # opts = Keyword.merge(@opts, opts)
        GenServer.start_link(RabbitMQStream.Publisher.Lifecycle, opts, name: __MODULE__)
      end

      def publish(message) do
        value = filter_value(message)

        message = encode!(message)

        GenServer.cast(__MODULE__, {:publish, {message, value}})
      end

      def stop() do
        GenServer.stop(__MODULE__)
      end

      def before_start(_opts, state), do: state
      def filter_value(_), do: nil

      unquote(
        # We need this piece of logic so we can garantee that the 'encode!/1' call is executed
        # by the caller process, not the Publisher process itself.
        if serializer != nil do
          quote do
            def encode!(message), do: unquote(serializer).encode!(message)
          end
        else
          quote do
            def encode!(message), do: message
          end
        end
      )

      defoverridable RabbitMQStream.Publisher
    end
  end

  defstruct [
    :publishing_id,
    :reference_name,
    :connection,
    :stream_name,
    :sequence,
    :serializer,
    :publisher_module,
    :id
  ]

  @type t :: %__MODULE__{
          publishing_id: String.t(),
          reference_name: String.t(),
          connection: GenServer.server(),
          stream_name: String.t(),
          sequence: non_neg_integer() | nil,
          serializer: (term() -> String.t()) | nil,
          id: String.t() | nil,
          publisher_module: module()
        }

  @type options :: [option()]

  @type option ::
          {:stream_name, String.t()}
          | {:reference_name, String.t()}
          | {:connection, GenServer.server()}
  @optional_callbacks [before_start: 2, filter_value: 1]

  @doc """
  Optional callback that is called after the process has started, but before the
  publisher has declared itself and fetched its most recent `publishing_id`.

  This is usefull for setup logic, such as creating the Stream, if it doesn't yet exists.
  """
  @callback before_start(options(), t()) :: t()

  @doc """
  Callback used to fetch the filter value for a message

  Example:
        @impl true
        def filter_value(message) do
          message["key"]
        end
  """
  @callback filter_value(term()) :: String.t()
end
