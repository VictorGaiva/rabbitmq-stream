defmodule RabbitMQStream.SuperPublisher do
  @moduledoc """
  A Superpublisher spawns a Publisher process for each partition of the stream,
  and uses the `partition/2` callback to forward a publish command to the
  producer of the partition.

  It accepts the same options as a Publisher, plus the following:

  * `:super_stream` - the name of the super stream
  * `:partitions` - the number of partitions

  All the publishers use the same provided connection, and are supervised by a
  DynamicSupervisor.

  You can optionally implement a `partition/2` callback to compute the target
  partition for a given message. By default, the partition is computed using
  `:erlang.phash2/2`.


  ## Setup

  To start a Superpublisher, you need to make sure that each stream/partition
  is created beforehand. As of RabbitMQ 3.11.x and 3.12.x, this can only be done
  using an [AMQP Client, RabbitMQ Management or with the RabbitMQ CLI.](https://www.rabbitmq.com/streams.html#super-streams).

  The easiest way to do this is to use the RabbitMQ CLI:

  `$ rabbitmq-streams add_super_stream invoices --partitions 3`

  As of 3.13.0-rc4 RabbitMQ team is in the process of implementing [Partition and Route commands](https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_stream/docs/PROTOCOL.adoc#route)
  using the Stream Protocol itself, which are already initially implemented
  by the `RabbitMQStream.Connection` module, but are not yet fully functional.

  """
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
