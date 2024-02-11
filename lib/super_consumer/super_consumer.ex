defmodule RabbitMQStream.SuperConsumer do
  @moduledoc """
  A Superconsumer spawns a Consumer process for each partition of the stream.

  It accepts the same options as a Consumer, plus the following:

  * `:super_stream` - the name of the super stream
  * `:partitions` - the number of partitions


  All the consumers use the same provided connection, and are supervised by a
  DynamicSupervisor.

  """

  defmacro __using__(opts) do
    defaults = Application.get_env(:rabbitmq_stream, :defaults, [])

    serializer = Keyword.get(opts, :serializer, Keyword.get(defaults, :serializer))
    opts = Keyword.put_new(opts, :partitions, Keyword.get(defaults, :partitions, 1))

    quote location: :keep do
      @opts unquote(opts)
      @behaviour RabbitMQStream.Consumer

      use Supervisor

      def start_link(opts) do
        opts =
          Application.get_env(:rabbitmq_stream, __MODULE__, [])
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)

        Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
      end

      def child_spec(opts) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
      end

      @impl true
      def init(opts) do
        {opts, consumer_opts} = Keyword.split(opts, [:super_stream, :connection])
        consumer_opts = Keyword.put(consumer_opts, :connection, opts[:connection])

        children = [
          {Registry, keys: :unique, name: __MODULE__.Registry},
          {DynamicSupervisor, strategy: :one_for_one, name: __MODULE__.DynamicSupervisor},
          {RabbitMQStream.SuperConsumer.Manager,
           opts ++
             [
               name: __MODULE__.Manager,
               dynamic_supervisor: __MODULE__.DynamicSupervisor,
               registry: __MODULE__.Registry,
               consumer_module: __MODULE__,
               partitions: @opts[:partitions],
               consumer_opts: consumer_opts
             ]}
        ]

        Supervisor.init(children, strategy: :one_for_all)
      end

      def before_start(_opts, state), do: state

      unquote(
        if serializer != nil do
          quote do
            def decode!(message), do: unquote(serializer).decode!(message)
          end
        else
          quote do
            def decode!(message), do: message
          end
        end
      )
    end
  end

  defstruct [
    :super_stream,
    :partitions,
    :registry,
    :dynamic_supervisor,
    :consumer_module,
    :consumer_opts,
    :connection
  ]

  @type t :: %__MODULE__{
          connection: GenServer.server(),
          super_stream: String.t(),
          partitions: non_neg_integer(),
          dynamic_supervisor: module(),
          consumer_module: module(),
          registry: module(),
          consumer_opts: [RabbitMQStream.Consumer.option()] | nil
        }

  @type super_consumer_option ::
          {:super_stream, String.t()}
          | {:partitions, non_neg_integer()}
          | RabbitMQStream.Consumer.option()
end
