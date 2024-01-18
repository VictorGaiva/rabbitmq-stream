defmodule RabbitMQStream.SuperProducer do
  @moduledoc """
  A Superproducer spawns a Producer process for each partition of the stream,
  and uses the `partition/2` callback to forward a publish command to the
  producer of the partition.

  It accepts the same options as a Producer, plus the following:

  * `:super_stream` - the name of the super stream
  * `:partitions` - the number of partitions

  All the producers use the same provided connection, and are supervised by a
  DynamicSupervisor.

  You can optionally implement a `partition/2` callback to compute the target
  partition for a given message. By default, the partition is computed using
  `:erlang.phash2/2`.


  ## Setup

  To start a Superproducer, you need to make sure that each stream/partition
  is created beforehand. As of RabbitMQ 3.11.x and 3.12.x, this can only be done
  using an [AMQP Client, RabbitMQ Management or with the RabbitMQ CLI.](https://www.rabbitmq.com/streams.html#super-streams).

  The easiest way to do this is to use the RabbitMQ CLI:

  `$ rabbitmq-streams add_super_stream invoices --partitions 3`

  As of RabbitMQ 3.13.x, you can also create a super stream using the
  `RabbitMQStream.Connection.create_super_stream/4`.

  """
  defmacro __using__(opts) do
    defaults = Application.get_env(:rabbitmq_stream, :defaults, [])

    serializer = Keyword.get(opts, :serializer, Keyword.get(defaults, :serializer))
    opts = Keyword.put_new(opts, :partitions, Keyword.get(defaults, :partitions, 1))

    quote do
      @opts unquote(opts)
      @behaviour RabbitMQStream.Producer

      use Supervisor

      def start_link(opts) do
        opts = Keyword.merge(opts, @opts)
        Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
      end

      @impl true
      def init(opts) do
        {opts, producer_opts} = Keyword.split(opts, [:super_stream])

        children = [
          {Registry, keys: :unique, name: __MODULE__.Registry},
          {DynamicSupervisor, strategy: :one_for_one, name: __MODULE__.DynamicSupervisor},
          {RabbitMQStream.SuperProducer.Manager,
           opts ++
             [
               name: __MODULE__.Manager,
               dynamic_supervisor: __MODULE__.DynamicSupervisor,
               registry: __MODULE__.Registry,
               producer_module: __MODULE__,
               partitions: @opts[:partitions],
               producer_opts: producer_opts
             ]}
        ]

        Supervisor.init(children, strategy: :one_for_all)
      end

      def publish(message) do
        value = filter_value(message)

        message = encode!(message)

        partition = partition(message, @opts[:partitions])

        GenServer.cast(
          {:via, Registry, {__MODULE__.Registry, partition}},
          {:publish, {message, value}}
        )
      end

      def stop() do
        GenServer.stop(__MODULE__)
      end

      def partition(message, partitions) do
        :erlang.phash2(message, partitions)
      end

      def before_start(_opts, state), do: state
      def filter_value(_), do: nil

      unquote(
        # We need this piece of logic so we can garantee that the 'encode!/1' call is executed
        # by the caller process, not the Producer process itself.
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

      defoverridable RabbitMQStream.Producer
    end
  end

  defstruct [
    :super_stream,
    :partitions,
    :dynamic_supervisor,
    :producer_module,
    :registry,
    :producer_opts
  ]

  @type t :: %__MODULE__{
          super_stream: String.t(),
          partitions: non_neg_integer(),
          dynamic_supervisor: module(),
          producer_module: module(),
          registry: module(),
          producer_opts: [RabbitMQStream.Producer.producer_option()] | nil
        }

  @type super_producer_option ::
          {:super_stream, String.t()}
          | {:partitions, non_neg_integer()}
          | RabbitMQStream.Producer.producer_option()
end
