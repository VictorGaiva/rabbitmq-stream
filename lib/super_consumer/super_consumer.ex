defmodule RabbitMQStream.SuperConsumer do
  defmacro __using__(opts) do
    quote do
      @opts unquote(opts)
      @behaviour RabbitMQStream.SuperConsumer

      use Supervisor

      def start_link(opts) do
        Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
      end

      @impl true
      def init(opts) do
        children = [
          {Registry, keys: :unique, name: __MODULE__.Registry},
          {DynamicSupervisor, strategy: :one_for_one, name: __MODULE__.DynamicSupervisor},
          {RabbitMQStream.SuperConsumer.Manager,
           opts ++
             [name: __MODULE__.Manager, dynamic_supervisor: __MODULE__.DynamicSupervisor, registry: __MODULE__.Registry]}
        ]

        # We use `one_for_all` because if the DynamicSupervisor shuts down for some reason, we must be able to
        # re-build all the children from the Manager
        Supervisor.init(children, strategy: :one_for_all)
      end
    end
  end

  @optional_callbacks handle_chunk: 1, handle_chunk: 2
  @callback handle_chunk(chunk :: RabbitMQStream.OsirisChunk.t()) :: term()
  @callback handle_chunk(chunk :: RabbitMQStream.OsirisChunk.t(), state :: RabbitMQStream.Consumer.t()) :: term()

  defstruct [
    :super_stream,
    :partitions,
    :connection,
    :consumer_opts,
    :registry,
    :dynamic_supervisor
  ]

  @type t :: %__MODULE__{
          super_stream: String.t(),
          partitions: [String.t()],
          connection: module(),
          dynamic_supervisor: module(),
          registry: module(),
          consumer_opts: [RabbitMQStream.Consumer.consumer_option()] | nil
        }

  @type super_consumer_option ::
          {:super_stream, String.t()}
          | {:partitions, [String.t()]}
          | {:connection, module()}
          | {:consumer_opts, [RabbitMQStream.Consumer.consumer_option()]}
end
