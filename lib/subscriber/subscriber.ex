defmodule RabbitMQStream.Subscriber do
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts], location: :keep do
      use GenServer
      @behaviour RabbitMQStream.Subscriber

      @opts opts

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

        strategies = Keyword.get(opts, :offset_strategy, [RabbitMQStream.Subscriber.OffsetTracking.CountStrategy])

        offset_reference = Keyword.get(opts, :offset_reference, Atom.to_string(__MODULE__))

        state = %RabbitMQStream.Subscriber{
          stream_name: stream_name,
          connection: connection,
          offset_reference: offset_reference,
          private: opts[:private],
          offset_tracking: init_strategies(strategies, opts)
        }

        {:ok, state, {:continue, opts}}
      end

      @impl true
      def handle_continue(opts, state) do
        initial_offset = Keyword.get(opts, :initial_offset) || raise(":initial_offset is required")
        initial_credit = Keyword.get(opts, :initial_credit) || 50_000

        last_offset =
          case state.connection.query_offset(state.stream_name, state.offset_reference) do
            {:ok, offset} ->
              {:offset, offset}

            _ ->
              initial_offset
          end

        case state.connection.subscribe(state.stream_name, self(), last_offset, initial_credit) do
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
      def handle_info({:message, %RabbitMQStream.Message.Data.DeliverData{} = message}, state) do
        cond do
          function_exported?(__MODULE__, :handle_chunk, 1) ->
            apply(__MODULE__, :handle_chunk, [message])

          function_exported?(__MODULE__, :handle_chunk, 2) ->
            apply(__MODULE__, :handle_chunk, [message, state])

          true ->
            raise "handle_chunk/1 or handle_chunk/2 must be implemented"
        end

        offset_tracking =
          for {strategy, track_state} <- state.offset_tracking do
            if function_exported?(strategy, :after_chunk, 3) do
              {strategy, strategy.after_chunk(track_state, message, state)}
            else
              {strategy, track_state}
            end
          end

        state =
          %{state | offset_tracking: offset_tracking, last_offset: message.osiris_chunk.chunk_first_offset}
          |> run_offset_tracking()

        {:noreply, state}
      end

      def handle_info(:run_offset_tracking, state) do
        {:noreply, run_offset_tracking(state)}
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

      defp init_strategies(strategies, subscriber_opts) do
        strategies
        |> List.wrap()
        |> Enum.map(fn
          {strategy, opts} when is_list(opts) and is_atom(strategy) ->
            {strategy, strategy.init(Keyword.merge(subscriber_opts, opts))}

          strategy when is_atom(strategy) ->
            {strategy, strategy.init(subscriber_opts)}
        end)
      end
    end
  end

  @moduledoc """
    Used to declare a Subscriber module.

    ## Usage

        defmodule MyApp.MySubscriber do
          use RabbitMQStream.Subscriber,
            connection: MyApp.MyConnection,
            stream_name: "my_stream",
            initial_offset: :first

          @impl true
          def handle_chunk(_chunk, _subscriber) do
            :ok
          end
        end


     ## Parameters

      * `:connection` - The connection module to use. This is required.
      * `:stream_name` - The name of the stream to subscribe to. This is required.
      * `:initial_offset` - The initial offset to subscribe from. This is required.
      * `:initial_credit` - The initial credit to request from the server. Defaults to `50_000`.
      * `:offset_strategy` - Offset tracking strategies to use. Defaults to `[RabbitMQStream.Subscriber.OffsetTracking.CountStrategy]`.
      * `:private` - Private data that can hold any value, and is passed to the `handle_chunk/2` callback.


  """

  @optional_callbacks handle_chunk: 1, handle_chunk: 2

  @doc """
    Callback invoked when a chunk is received.
  """
  @callback handle_chunk(chunk :: RabbitMQStream.Message.Data.DeliverData.t()) :: term()
  @callback handle_chunk(chunk :: RabbitMQStream.Message.Data.DeliverData.t(), state :: t()) :: term()

  defstruct [
    :offset_reference,
    :connection,
    :stream_name,
    :offset_tracking,
    :id,
    :last_offset,
    :private
  ]

  @type t :: %__MODULE__{
          offset_reference: String.t(),
          connection: RabbitMQStream.Connection.t(),
          stream_name: String.t(),
          id: non_neg_integer() | nil,
          offset_tracking: OffsetAutoTracking.t(),
          last_offset: non_neg_integer() | nil,
          private: any()
        }

  @type subscriber_option ::
          {:offset_reference, String.t()}
          | {:connection, RabbitMQStream.Connection.t()}
          | {:stream_name, String.t()}
          | {:initial_offset, RabbitMQStream.Connection.offset()}
          | {:initial_credit, non_neg_integer()}
          | {:offset_strategy, [OffsetAutoTracking.auto_tracking_strategy_option()]}
          | {:private, any()}

  @type opts :: [subscriber_option()]
end
