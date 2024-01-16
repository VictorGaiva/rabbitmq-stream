defmodule RabbitMQStream.SuperConsumer do
  defmacro __using__(opts) do
    quote do
      @opts unquote(opts)
      @behaviour RabbitMQStream.Consumer

      use Supervisor

      def start_link(opts) do
        opts = Keyword.merge(opts, @opts)
        Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
      end

      @impl true
      def init(opts) do
        children = [
          {Registry, keys: :unique, name: __MODULE__.Registry},
          {DynamicSupervisor, strategy: :one_for_one, name: __MODULE__.DynamicSupervisor},
          {RabbitMQStream.SuperConsumer.Manager,
           opts ++
             [
               name: __MODULE__.Manager,
               dynamic_supervisor: __MODULE__.DynamicSupervisor,
               registry: __MODULE__.Registry,
               consumer_module: __MODULE__
             ]}
        ]

        # We use `one_for_all` because if the DynamicSupervisor shuts down for some reason, we must be able to
        # re-build all the children from the Manager
        Supervisor.init(children, strategy: :one_for_all)
      end
    end
  end

  defstruct [
    :super_stream,
    :partitions,
    :connection,
    :consumer_opts,
    :registry,
    :dynamic_supervisor,
    :consumer_module
  ]

  @type t :: %__MODULE__{
          super_stream: String.t(),
          partitions: non_neg_integer(),
          connection: module(),
          dynamic_supervisor: module(),
          consumer_module: module(),
          registry: module(),
          consumer_opts: [RabbitMQStream.Consumer.consumer_option()] | nil
        }

  @type super_consumer_option ::
          {:super_stream, String.t()}
          | {:partitions, non_neg_integer()}
          | {:connection, module()}
          | {:consumer_opts, [RabbitMQStream.Consumer.consumer_option()]}
end
