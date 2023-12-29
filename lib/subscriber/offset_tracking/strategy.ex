defmodule RabbitMQStream.Subscriber.OffsetTracking.Strategy do
  @type t :: module()

  @moduledoc """
  Behavior for offset tracking strategies.

  ## Existing Strategies

  * `RabbitMQStream.Subscriber.OffsetTracking.IntervalStrategy`
  * `RabbitMQStream.Subscriber.OffsetTracking.CountStrategy`

  """

  @optional_callbacks [after_chunk: 3]

  @doc """
  Initializes the strategy state.

  ## Parameters
  * `opts` - a keyword list of the options passed to the subscriber,
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
              subscription :: RabbitMQStream.Subscriber.t()
            ) ::
              term()

  @doc """
  Callback responsible for deciding whether to store the offset, based on its internal state.

  ## Parameters

  * `state` - the state of the strategy
  * `subscription` - the state of the owner subscription process
  """
  @callback run(state :: term(), subscription :: RabbitMQStream.Subscriber.t()) ::
              {:store, state :: term()} | {:skip, state :: term()}
end
