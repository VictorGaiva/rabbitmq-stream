defmodule RabbitMQStream.Consumer.OffsetTracking do
  @type t :: module()

  @moduledoc """
  Behavior for offset tracking strategies.
  If you pass multiple strategies to the consumer, which will be executed in order, and
  and halt after the first one that returns a `:store` request.

  # Existing Strategies
  You can use the default strategies by passing a shorthand alias:

  * `interval` : `RabbitMQStream.Consumer.OffsetTracking.IntervalStrategy`
  * `count` : `RabbitMQStream.Consumer.OffsetTracking.CountStrategy`

  """

  @optional_callbacks [after_chunk: 3]

  @doc """
  Initializes the strategy state.

  # Parameters
  * `opts` - a keyword list of the options passed to the consumer,
      merged with the options passed to the strategy itself.
  """
  @callback init(opts :: term()) :: term()

  @doc """
  Optional Callback executed after every chunk, which can be used to update the state.

  Useful, for example, to store the offset after a certain number of messages.

  """
  @callback after_chunk(
              state :: term(),
              chunk :: RabbitMQStream.OsirisChunk.t(),
              subscription :: RabbitMQStream.Consumer.t()
            ) ::
              term()

  @doc """
  Callback responsible for deciding whether to store the offset, based on its internal state.

  # Parameters

  * `state` - the state of the strategy
  * `subscription` - the state of the owner subscription process
  """
  @callback run(state :: term(), subscription :: RabbitMQStream.Consumer.t()) ::
              {:store, state :: term()} | {:skip, state :: term()}

  @defaults %{
    count: RabbitMQStream.Consumer.OffsetTracking.CountStrategy,
    interval: RabbitMQStream.Consumer.OffsetTracking.IntervalStrategy
  }

  require Logger

  @doc false
  def init(strategies, extra_opts \\ []) do
    strategies
    |> List.wrap()
    |> Enum.map(fn
      {strategy, opts} when is_list(opts) and is_atom(strategy) ->
        strategy = @defaults[strategy] || strategy

        {strategy, strategy.init(Keyword.merge(extra_opts, opts))}

      strategy when is_atom(strategy) ->
        strategy = @defaults[strategy] || strategy

        {strategy, strategy.init(extra_opts)}
    end)
  end

  @doc false
  def run(%RabbitMQStream.Consumer{last_offset: nil} = state), do: state

  def run(%RabbitMQStream.Consumer{} = state) do
    {_, offset_tracking} =
      Enum.reduce(
        state.offset_tracking,
        {:cont, []},
        fn
          {strategy, track_state}, {:cont, acc} ->
            case strategy.run(track_state, state) do
              {:store, new_track_state} ->
                Logger.debug(
                  "Storing offset for stream: #{state.stream_name} with offset: #{state.last_offset}. Strategy: #{strategy}"
                )

                RabbitMQStream.Connection.store_offset(
                  state.connection,
                  state.stream_name,
                  state.offset_reference,
                  state.last_offset
                )

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
end
