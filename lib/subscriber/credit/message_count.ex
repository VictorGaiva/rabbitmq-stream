defmodule RabbitMQStream.Subscriber.FlowControl.MessageCount do
  @behaviour RabbitMQStream.Subscriber.FlowControl.Strategy

  @moduledoc """
  Message Count Strategy

  Adds credits after the amount of consumed credit reaches a certain threshold.

  # Usage
      defmodule MyApp.MySubscriber do
        alias RabbitMQStream.Subscriber.FlowControl

        use RabbitMQStream.Subscriber,
          offset_tracking: [FlowControl.MessageCount, credit_after: {:count, 1}]

        @impl true
        def handle_chunk(_chunk, _subscriber) do
          :ok
        end
      end

  # Parameters

  * `credit_after` - The type of computation performed to decide whether to add more credit.
      Can be one of:
      * `{:count, amount}` - adds the amount in credits after the specified is consumed
      * `{:ratio, ratio}` - credits the missing amount after the ratio of remaining credits reaches the threshold

  Defaults to `{:count, 1}`.

  """

  def init(opts \\ []) do
    Keyword.get(opts, :credit_after, {:count, 1})
  end

  def run({:count, amount}, %{initial_credit: initial, credits: credits}) when initial - credits >= amount do
    {:credit, div(initial - credits, amount) * amount, {:count, amount}}
  end

  def run({:ratio, ratio}, %{initial_credit: initial, credits: credits}) when initial - credits >= initial * ratio do
    {:credit, initial - credits, {:ratio, ratio}}
  end

  def run(state, _) do
    {:skip, state}
  end
end
