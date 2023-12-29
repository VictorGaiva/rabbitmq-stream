defmodule RabbitMQStream.Subscriber do
  @moduledoc """
    Used to declare a Persistent Subscriber module. It is able to process
    chunks by implementing the `handle_chunk/1` or `handle_chunk/2` callbacks.

    ## Usage

        defmodule MyApp.MySubscriber do
          use RabbitMQStream.Subscriber,
            connection: MyApp.MyConnection,
            stream_name: "my_stream",
            initial_offset: :first

          @impl true
          def handle_chunk(%RabbitMQStream.OsirisChunk{} = _chunk, _subscriber) do
            :ok
          end
        end


    ## Parameters

      * `:connection` - The connection module to use. This is required.
      * `:stream_name` - The name of the stream to subscribe to. This is required.
      * `:initial_offset` - The initial offset to subscribe from. This is required.
      * `:initial_credit` - The initial credit to request from the server. Defaults to `50_000`.
      * `:offset_strategy` - Offset tracking strategies to use. Defaults to `[RabbitMQStream.Subscriber.OffsetTracking.CountStrategy]`.
      * `:flow_control` - Flow control strategy to use. Defaults to `RabbitMQStream.Subscriber.FlowControl.MessageCount`.
      * `:private` - Private data that can hold any value, and is passed to the `handle_chunk/2` callback.


    ## Offset Tracking

    The subscriber is able to track its progress in the stream by storing its
    latests offset in the stream. Check [Offset Tracking with RabbitMQ Streams(https://blog.rabbitmq.com/posts/2021/09/rabbitmq-streams-offset-tracking/) for more information on
    how offset tracking works.

    The subscriber can be configured to use different offset tracking strategies,
    which decide when to store the offset in the stream. You can implement your
    own strategy by implementing the `RabbitMQStream.Subscriber.OffsetTracking.Strategy`
    behaviour, and passing it to the `:offset_strategy` option. It defaults to
    `RabbitMQStream.Subscriber.OffsetTracking.CountStrategy`, which stores the
    offset after, by default, every 50_000 messages.

    ## Flow Control

    The RabbitMQ Streams server requires that the subscriber declares how many
    messages it is able to process at a time. This is done by informing an amount
    of 'credits' to the server. After every chunk is sent, one credit is consumed,
    and the server will send messages only if there are credits available.

    We can configure the subscriber to automatically request more credits based on
    a strategy. By default it uses the `RabbitMQStream.Subscriber.FlowControl.MessageCount`,
    which requests 1 additional credit for every 1 processed chunk. Please check
    the RabbitMQStream.Subscriber.FlowControl.MessageCount module for more information.

    You can also call `RabbitMQStream.Subscriber.credit/2` to manually add more
    credits to the subscription, or implement your own strategy by implementing
    the `RabbitMQStream.Subscriber.FlowControl.Strategy` behaviour, and passing
    it to the `:flow_control` option.

    You can find more information on the [RabbitMQ Streams documentation](https://www.rabbitmq.com/stream.html#flow-control).

    If you want an external process to be fully in control of the flow control
    of a subscriber, you can set the `:flow_control` option to `false`. Then
    you can call `RabbitMQStream.Subscriber.credit/2` to manually add more
    credits to the subscription.

  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts], location: :keep do
      use GenServer
      @behaviour RabbitMQStream.Subscriber

      @opts opts

      def credit(amount) do
        GenServer.cast(__MODULE__, {:credit, amount})
      end

      def get_credits() do
        GenServer.call(__MODULE__, :get_credits)
      end

      def start_link(opts \\ []) do
        opts =
          Application.get_env(:rabbitmq_stream, __MODULE__, [])
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)

        # opts = Keyword.merge(@opts, opts)
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
      end

      @impl true
      def init(opts \\ []) do
        connection = Keyword.get(opts, :connection) || raise(":connection is required")
        stream_name = Keyword.get(opts, :stream_name) || raise(":stream_name is required")
        initial_credit = Keyword.get(opts, :initial_credit, 50_000)

        offset_tracking = Keyword.get(opts, :offset_strategy, [RabbitMQStream.Subscriber.OffsetTracking.CountStrategy])
        flow_control = Keyword.get(opts, :flow_control, RabbitMQStream.Subscriber.FlowControl.MessageCount)

        offset_reference = Keyword.get(opts, :offset_reference, Atom.to_string(__MODULE__))

        state = %RabbitMQStream.Subscriber{
          stream_name: stream_name,
          connection: connection,
          offset_reference: offset_reference,
          private: opts[:private],
          offset_tracking: init_offset_tracking(offset_tracking, opts),
          flow_control: init_flow_control(flow_control, opts),
          credits: initial_credit,
          initial_credit: initial_credit
        }

        {:ok, state, {:continue, opts}}
      end

      @impl true
      def handle_continue(opts, state) do
        initial_offset = Keyword.get(opts, :initial_offset) || raise(":initial_offset is required")

        last_offset =
          case state.connection.query_offset(state.stream_name, state.offset_reference) do
            {:ok, offset} ->
              {:offset, offset}

            _ ->
              initial_offset
          end

        case state.connection.subscribe(state.stream_name, self(), last_offset, state.initial_credit) do
          {:ok, id} ->
            last_offset =
              case last_offset do
                {:offset, offset} ->
                  offset

                _ ->
                  nil
              end

            {:noreply, %{state | id: id, last_offset: last_offset}}

          err ->
            {:stop, err, state}
        end
      end

      @impl true
      def terminate(_reason, state) do
        state.connection.unsubscribe(state.id)
        :ok
      end

      @impl true
      def handle_info({:chunk, %RabbitMQStream.OsirisChunk{} = chunk}, state) do
        cond do
          function_exported?(__MODULE__, :handle_chunk, 1) ->
            apply(__MODULE__, :handle_chunk, [chunk])

          function_exported?(__MODULE__, :handle_chunk, 2) ->
            apply(__MODULE__, :handle_chunk, [chunk, state])

          true ->
            raise "handle_chunk/1 or handle_chunk/2 must be implemented"
        end

        offset_tracking =
          for {strategy, track_state} <- state.offset_tracking do
            if function_exported?(strategy, :after_chunk, 3) do
              {strategy, strategy.after_chunk(track_state, chunk, state)}
            else
              {strategy, track_state}
            end
          end

        credit = state.credits - chunk.num_entries

        state =
          %{state | offset_tracking: offset_tracking, last_offset: chunk.chunk_id, credits: credit}

        state = state |> run_offset_tracking() |> run_flow_control()

        {:noreply, state}
      end

      def handle_info(:run_offset_tracking, state) do
        {:noreply, run_offset_tracking(state)}
      end

      def handle_info(:run_flow_control, state) do
        {:noreply, run_flow_control(state)}
      end

      @impl true
      def handle_cast({:credit, amount}, state) do
        state.connection.credit(state.id, amount)
        {:noreply, %{state | credits: state.credits + amount}}
      end

      @impl true
      def handle_call(:get_credits, _from, state) do
        {:reply, state.credits, state}
      end

      defp run_offset_tracking(%{last_offset: nil} = state), do: state

      defp run_offset_tracking(state) do
        {_, offset_tracking} =
          Enum.reduce(
            state.offset_tracking,
            {:cont, []},
            fn
              {strategy, track_state}, {:cont, acc} ->
                case strategy.run(track_state, state) do
                  {:store, new_track_state} ->
                    state.connection.store_offset(state.stream_name, state.offset_reference, state.last_offset)

                    {:halt, [{strategy, new_track_state} | acc]}

                  {:skip, new_track_state} ->
                    {:cont, [{strategy, new_track_state} | acc]}
                end

              state, {:halt, acc} ->
                {:halt, [state | acc]}
            end
          )

        %{state | offset_tracking: offset_tracking}
      end

      defp run_flow_control(%{flow_control: {strategy, flow_state}} = state) do
        case strategy.run(flow_state, state) do
          {:credit, amount, new_flow_control} ->
            state.connection.credit(state.id, amount)
            %{state | flow_control: {strategy, new_flow_control}, credits: state.credits + amount}

          {:skip, new_flow_control} ->
            %{state | flow_control: {strategy, new_flow_control}}
        end
      end

      defp run_flow_control(%{flow_control: false} = state), do: state

      defp init_offset_tracking(strategies, subscriber_opts) do
        strategies
        |> List.wrap()
        |> Enum.map(fn
          {strategy, opts} when is_list(opts) and is_atom(strategy) ->
            {strategy, strategy.init(Keyword.merge(subscriber_opts, opts))}

          strategy when is_atom(strategy) ->
            {strategy, strategy.init(subscriber_opts)}
        end)
      end

      defp init_flow_control({strategy, opts}, subscriber_opts) do
        {strategy, strategy.init(Keyword.merge(subscriber_opts, opts))}
      end

      defp init_flow_control(false, _) do
        false
      end

      defp init_flow_control(strategy, subscriber_opts) do
        {strategy, strategy.init(subscriber_opts)}
      end
    end
  end

  @optional_callbacks handle_chunk: 1, handle_chunk: 2

  @doc """
    The callback that is invoked when a chunk is received.

    Each chunk contains a list of potentially many data entries, along with
    metadata about the chunk itself. The callback is invoked once for each
    chunk received.

    Optionally if you implement `handle_chunk/2`, it also passes the current
    state of the subscriber. It can be used to access the `private` field
    passed to `start_link/1`, and other fields.

    The return value is ignored.
  """
  @callback handle_chunk(chunk :: RabbitMQStream.OsirisChunk.t()) :: term()
  @callback handle_chunk(chunk :: RabbitMQStream.OsirisChunk.t(), state :: t()) :: term()

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
    :private
  ]

  @type t :: %__MODULE__{
          offset_reference: String.t(),
          connection: RabbitMQStream.Connection.t(),
          stream_name: String.t(),
          id: non_neg_integer() | nil,
          offset_tracking: [{RabbitMQStream.Subscriber.OffsetTracking.Strategy.t(), term()}],
          flow_control: {RabbitMQStream.Subscriber.FlowControl.Strategy.t(), term()},
          last_offset: non_neg_integer() | nil,
          private: any(),
          credits: non_neg_integer(),
          initial_credit: non_neg_integer()
        }

  @type subscriber_option ::
          {:offset_reference, String.t()}
          | {:connection, RabbitMQStream.Connection.t()}
          | {:stream_name, String.t()}
          | {:initial_offset, RabbitMQStream.Connection.offset()}
          | {:initial_credit, non_neg_integer()}
          | {:offset_strategy, [{RabbitMQStream.Subscriber.OffsetTracking.Strategy.t(), term()}]}
          | {:flow_control, {RabbitMQStream.Subscriber.FlowControl.Strategy.t(), term()}}
          | {:private, any()}

  @type opts :: [subscriber_option()]
end
