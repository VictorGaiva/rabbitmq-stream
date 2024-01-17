defmodule RabbitMQStream.SuperPublisher do
  defmacro __using__(opts) do
    defaults = Application.get_env(:rabbitmq_stream, :defaults, [])

    serializer = Keyword.get(opts, :serializer, Keyword.get(defaults, :serializer))
    opts = Keyword.put_new(opts, :partitions, Keyword.get(defaults, :partitions, 1))

    quote do
      @opts unquote(opts)
      @behaviour RabbitMQStream.Publisher

      use Supervisor

      def start_link(opts) do
        opts = Keyword.merge(opts, @opts)
        Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
      end

      @impl true
      def init(opts) do
        {opts, publisher_opts} = Keyword.split(opts, [:super_stream])

        children = [
          {Registry, keys: :unique, name: __MODULE__.Registry},
          {DynamicSupervisor, strategy: :one_for_one, name: __MODULE__.DynamicSupervisor},
          {RabbitMQStream.SuperPublisher.Manager,
           opts ++
             [
               name: __MODULE__.Manager,
               dynamic_supervisor: __MODULE__.DynamicSupervisor,
               registry: __MODULE__.Registry,
               publisher_module: __MODULE__,
               partitions: @opts[:partitions],
               publisher_opts: publisher_opts
             ]}
        ]

        Supervisor.init(children, strategy: :one_for_all)
      end

      def publish(message) do
        value = filter_value(message)

        message = encode!(message)

        partition = byte_size(message) |> rem(@opts[:partitions])

        GenServer.cast(
          {:via, Registry, {__MODULE__.Registry, partition}},
          {:publish, {message, value}}
        )
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
    :super_stream,
    :partitions,
    :dynamic_supervisor,
    :publisher_module,
    :registry,
    :publisher_opts
  ]

  @type t :: %__MODULE__{
          super_stream: String.t(),
          partitions: non_neg_integer(),
          dynamic_supervisor: module(),
          publisher_module: module(),
          registry: module(),
          publisher_opts: [RabbitMQStream.Publisher.publisher_option()] | nil
        }

  @type super_publisher_option ::
          {:super_stream, String.t()}
          | {:partitions, non_neg_integer()}
          | RabbitMQStream.Publisher.publisher_option()
end
