defmodule RabbitMQStream.Subscriber.FlowControl.Strategy do
  @type t :: module()

  @moduledoc """
  Behavior for flow control strategies.

  ## Existing Strategies
  You can use the default strategies by passing a shorthand alias:

  * `count` : `RabbitMQStream.Subscriber.FlowControl.MessageCount`

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

  @defaults %{
    count: RabbitMQStream.Subscriber.FlowControl.MessageCount
  }
  @doc false
  def init([{strategy, opts}], subscriber_opts) do
    strategy = @defaults[strategy] || strategy

    {strategy, strategy.init(Keyword.merge(subscriber_opts, opts))}
  end

  def init(false, _) do
    false
  end

  def init(strategy, subscriber_opts) do
    strategy = @defaults[strategy] || strategy

    {strategy, strategy.init(subscriber_opts)}
  end

  @doc false
  def run(%RabbitMQStream.Subscriber{flow_control: {strategy, flow_state}} = state) do
    case strategy.run(flow_state, state) do
      {:credit, amount, new_flow_control} ->
        state.connection.credit(state.id, amount)
        %{state | flow_control: {strategy, new_flow_control}, credits: state.credits + amount}

      {:skip, new_flow_control} ->
        %{state | flow_control: {strategy, new_flow_control}}
    end
  end

  def run(%RabbitMQStream.Subscriber{flow_control: false} = state), do: state
end