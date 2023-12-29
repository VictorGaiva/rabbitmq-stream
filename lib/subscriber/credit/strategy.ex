defmodule RabbitMQStream.Subscriber.FlowControl.Strategy do
  @type t :: module()

  @moduledoc """
  Behavior for flow control strategies.

  ## Existing Strategies

  * `RabbitMQStream.Subscriber.FlowControl.MessageCount`

  """

  @doc """
  Initializes the strategy state.

  ## Parameters
  * `opts` - a keyword list of the options passed to the subscriber,
      merged with the options passed to the strategy itself.
  """
  @callback init(opts :: term()) :: term()

  @doc """
  Callback responsible for deciding whether to add more credit, based on its internal state.

  ## Parameters

  * `state` - the state of the strategy
  * `subscription` - the state of the owner subscription process
  """
  @callback run(state :: term(), subscription :: RabbitMQStream.Subscriber.t()) ::
              {:credit, amount :: non_neg_integer(), state :: term()} | {:skip, state :: term()}
end
