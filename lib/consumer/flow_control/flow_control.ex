defmodule RabbitMQStream.Consumer.FlowControl do
  @type t :: module()

  @moduledoc """
  Behavior for flow control strategies.

  # Existing Strategies
  You can use the default strategies by passing a shorthand alias:

  * `count` : `RabbitMQStream.Consumer.FlowControl.MessageCount`

  """

  @doc """
  Initializes the strategy state.

  # Parameters
  * `opts` - a keyword list of the options passed to the consumer,
      merged with the options passed to the strategy itself.
  """
  @callback init(opts :: term()) :: term()

  @doc """
  Callback responsible for deciding whether to add more credit, based on its internal state.

  # Parameters

  * `state` - the state of the strategy
  * `subscription` - the state of the owner subscription process
  """
  @callback run(state :: term(), subscription :: RabbitMQStream.Consumer.t()) ::
              {:credit, amount :: non_neg_integer(), state :: term()} | {:skip, state :: term()}

  @defaults %{
    count: RabbitMQStream.Consumer.FlowControl.MessageCount
  }

  require Logger

  @doc false
  def init([{strategy, opts}], consumer_opts) do
    strategy = @defaults[strategy] || strategy

    {strategy, strategy.init(Keyword.merge(consumer_opts, opts))}
  end

  def init(false, _) do
    false
  end

  def init(strategy, consumer_opts) do
    strategy = @defaults[strategy] || strategy

    {strategy, strategy.init(consumer_opts)}
  end

  @doc false
  def run(%RabbitMQStream.Consumer{flow_control: {strategy, flow_state}} = state) do
    case strategy.run(flow_state, state) do
      {:credit, amount, new_flow_control} ->
        Logger.debug("Adding credit: #{amount}. Strategy: #{inspect(strategy)}")

        RabbitMQStream.Connection.credit(state.connection, state.id, amount)
        %{state | flow_control: {strategy, new_flow_control}, credits: state.credits + amount}

      {:skip, new_flow_control} ->
        %{state | flow_control: {strategy, new_flow_control}}
    end
  end

  def run(%RabbitMQStream.Consumer{flow_control: false} = state), do: state
end
