defmodule RabbitMQStream.Consumer do
  @moduledoc """
  Used to declare a Persistent Consumer module. It is able to process
  chunks by implementing the `handle_chunk/1` or `handle_chunk/2` callbacks.

  # Usage

      defmodule MyApp.MyConsumer do
        use RabbitMQStream.Consumer,
          connection: MyApp.MyConnection,
          stream_name: "my_stream",
          initial_offset: :first

        @impl true
        def handle_chunk(%RabbitMQStream.OsirisChunk{} = _chunk, _consumer) do
          :ok
        end
      end


  # Parameters

  * `:connection` - The connection module to use. This is required.
  * `:stream_name` - The name of the stream to consume. This is required.
  * `:initial_offset` - The initial offset. This is required.
  * `:initial_credit` - The initial credit to request from the server. Defaults to `50_000`.
  * `:offset_tracking` - Offset tracking strategies to use. Defaults to `[count: [store_after: 50]]`.
  * `:flow_control` - Flow control strategy to use. Defaults to `[count: [credit_after: {:count, 1}]]`.
  * `:offset_reference` -
  * `:private` - Private data that can hold any value, and is passed to the `handle_chunk/2` callback.
  * `:serializer` - The module to use to decode the message. Defaults to `__MODULE__`,
    which means that the consumer will use the `decode!/1` callback to decode the message, which is implemented by default to return the message as is.

  * `:properties` - Define the properties of the subscription. Can only have one option at a time.
    * `:single_active_consumer`: set to `true` to enable [single active consumer](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams/) for this subscription.
    * `:super_stream`: set to the name of the super stream the subscribed is a partition of.
    * `:filter`: List of strings that define the value of the filter_key to match.
    * `:match_unfiltered`: whether to return messages without any filter value or not.


  # Offset Tracking

  The consumer is able to track its progress in the stream by storing its
  latests offset in the stream. Check [Offset Tracking with RabbitMQ Streams(https://blog.rabbitmq.com/posts/2021/09/rabbitmq-streams-offset-tracking/) for more information on
  how offset tracking works.

  The consumer can be configured to use different offset tracking strategies,
  which decide when to store the offset in the stream. You can implement your
  own strategy by implementing the `RabbitMQStream.Consumer.OffsetTracking`
  behaviour, and passing it to the `:offset_tracking` option. It defaults to
  `RabbitMQStream.Consumer.OffsetTracking.CountStrategy`, which stores the
  offset after, by default, every 50_000 messages.

  # Flow Control

  The RabbitMQ Streams server requires that the consumer declares how many
  messages it is able to process at a time. This is done by informing an amount
  of 'credits' to the server. After every chunk is sent, one credit is consumed,
  and the server will send messages only if there are credits available.

  We can configure the consumer to automatically request more credits based on
  a strategy. By default it uses the `RabbitMQStream.Consumer.FlowControl.MessageCount`,
  which requests 1 additional credit for every 1 processed chunk. Please check
  the RabbitMQStream.Consumer.FlowControl.MessageCount module for more information.

  You can also call `RabbitMQStream.Consumer.credit/2` to manually add more
  credits to the subscription, or implement your own strategy by implementing
  the `RabbitMQStream.Consumer.FlowControl` behaviour, and passing
  it to the `:flow_control` option.

  You can find more information on the [RabbitMQ Streams documentation](https://www.rabbitmq.com/stream.html#flow-control).

  If you want an external process to be fully in control of the flow control
  of a consumer, you can set the `:flow_control` option to `false`. Then
  you can call `RabbitMQStream.Consumer.credit/2` to manually add more
  credits to the subscription.


  # Configuration

  You can configure each consumer with:

      config :rabbitmq_stream, MyApp.MyConsumer,
        connection: MyApp.MyConnection,
        stream_name: "my_stream",
        initial_offset: :first,
        initial_credit: 50_000,
        offset_tracking: [count: [store_after: 50]],
        flow_control: [count: [credit_after: {:count, 1}]],
        serializer: Jason

  These options are overriden by the options passed to the `use` macro, which
  are overriden by the options passed to `start_link/1`.

  And also you can override the defaults of all consumers with:

        config :rabbitmq_stream, :defaults,
          consumer: [
            connection: MyApp.MyConnection,
            initial_credit: 50_000,
            # ...
          ],

  Globally configuring all consumers ignores the following options:

  * `:stream_name`
  * `:offset_reference`
  * `:private`

  """
  defmacro __using__(opts) do
    defaults = Application.get_env(:rabbitmq_stream, :defaults, [])

    serializer = Keyword.get(opts, :serializer, Keyword.get(defaults, :serializer))

    quote location: :keep do
      @opts unquote(opts)
      require Logger

      @behaviour RabbitMQStream.Consumer

      def start_link(opts \\ []) do
        unless !Keyword.has_key?(opts, :serializer) do
          Logger.warning("You can only pass `:serializer` option to compile-time options.")
        end

        opts =
          Application.get_env(:rabbitmq_stream, __MODULE__, [])
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)
          |> Keyword.put_new(:consumer_module, __MODULE__)
          |> Keyword.put(:name, __MODULE__)

        RabbitMQStream.Consumer.start_link(opts)
      end

      def child_spec(opts) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
      end

      def credit(amount) do
        GenServer.cast(__MODULE__, {:credit, amount})
      end

      def get_credits() do
        GenServer.call(__MODULE__, :get_credits)
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

      defoverridable RabbitMQStream.Consumer
    end
  end

  def start_link(opts \\ []) do
    opts =
      Application.get_env(:rabbitmq_stream, :defaults, [])
      |> Keyword.get(:consumer, [])
      |> Keyword.drop([:stream_name, :offset_reference, :private])
      |> Keyword.merge(opts)

    GenServer.start_link(RabbitMQStream.Consumer.LifeCycle, opts, name: opts[:name])
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  The callback that is invoked when a chunk is received.

  Each chunk contains a list of potentially many data entries, along with
  metadata about the chunk itself. The callback is invoked once for each
  chunk received.

  Optionally if you implement `handle_chunk/2`, it also passes the current
  state of the consumer. It can be used to access the `private` field
  passed to `start_link/1`, and other fields.

  The return value is ignored.
  """
  @callback handle_chunk(chunk :: RabbitMQStream.OsirisChunk.t()) :: term()
  @callback handle_chunk(chunk :: RabbitMQStream.OsirisChunk.t(), state :: t()) :: term()

  @doc """
  If the consumer has been defined with the 'single-active-consumer' parameter,
  this callback is invoked when the consumer is being upgraded to being the
  active one, or when downgraded to being an inactive one.

  When the flag parameter is set to ':upgrade', it means that the consumer is being
  upgraded to active and it must return the offset for where it wants to start
  consuming from the stream.

  When being downgraded, the offset returned by the callback is also sent
  to the server but, at the moment, is not being used in any way, and is only
  sent because the API requires. But this is actually a good moment to store
  the offset so that it can be retrieved by the other consumer that is being
  upgraded.
  """
  @callback handle_update(consumer :: t(), action :: :upgrade | :downgrade) ::
              {:ok, RabbitMQStream.Connection.offset()} | {:error, any()}

  @doc """
  Callback invoked on each message inside of a chunk.

  It can be used to decode the message from a binary format into a Map,
  or to use GZIP to decompress the content.

  You can also globally define a 'Serializer' module, that must implement
  the 'decode!/1' callback, at compile-time configuration so it is added
  to as the default callback.
  """
  @callback decode!(message :: String.t()) :: term()

  @doc """
  Callback invoked right before subscribing a consumer to the stream.
  Might be usefull for setup logic, like creating a stream if it doesn't yet exists.
  """
  @callback before_start(opts(), t()) :: t()

  @doc """
  Send a command to add the provided amount of credits to the consumer.

  The credits are tracked by the Server, but it is also stored internally
  on the Consumer state, which then can be retreived by calling 'get_credits/0'.

  Always returns :ok, and any errors when adding credits to a consumer are logged.
  """
  @callback credit(amount :: non_neg_integer()) :: :ok

  @doc """
  Returns the internally tracked amount of credits for the Consumer.
  """
  @callback get_credits() :: non_neg_integer()

  @optional_callbacks handle_chunk: 1,
                      handle_chunk: 2,
                      decode!: 1,
                      handle_update: 2,
                      before_start: 2,
                      get_credits: 0,
                      credit: 1

  defstruct [
    :offset_reference,
    :connection,
    :stream_name,
    :offset_tracking,
    :flow_control,
    :id,
    :last_offset,
    # We could have delegated the tracking of the credit to the strategy,
    #  by adding declaring a callback similar to `after_chunk/3`. But it seems
    #  reasonable to have a `credit` function to manually add more credits,
    #  which would them possibly cause the strategy to not work as expected.
    :credits,
    :initial_credit,
    :initial_offset,
    :private,
    :properties,
    :consumer_module
  ]

  @type t :: %__MODULE__{
          offset_reference: String.t(),
          connection: GenServer.server(),
          stream_name: String.t(),
          id: non_neg_integer() | nil,
          offset_tracking: [{RabbitMQStream.Consumer.OffsetTracking.t(), term()}],
          flow_control: {RabbitMQStream.Consumer.FlowControl.t(), term()},
          last_offset: non_neg_integer() | nil,
          private: any(),
          credits: non_neg_integer(),
          initial_credit: non_neg_integer(),
          initial_offset: RabbitMQStream.Connection.offset(),
          properties: [RabbitMQStream.Message.Types.ConsumerequestData.property()],
          consumer_module: module()
        }

  @type consumer_option ::
          {:offset_reference, String.t()}
          | {:connection, GenServer.server()}
          | {:stream_name, String.t()}
          | {:initial_offset, RabbitMQStream.Connection.offset()}
          | {:initial_credit, non_neg_integer()}
          | {:offset_tracking, [{RabbitMQStream.Consumer.OffsetTracking.t(), term()}]}
          | {:flow_control, {RabbitMQStream.Consumer.FlowControl.t(), term()}}
          | {:private, any()}
          | {:properties, [RabbitMQStream.Message.Types.ConsumerequestData.property()]}

  @type opts :: [consumer_option()]
end
