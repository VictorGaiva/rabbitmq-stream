defmodule RabbitMQStream.Consumer.OffsetTracking.IntervalStrategy do
  @behaviour RabbitMQStream.Consumer.OffsetTracking

  @moduledoc """
  Interval Strategy

  Runs a timer every `interval` milliseconds, and sends the parent process
  a `:run_offset_tracking` message.

  Since we also execute the strategy after every message, we also reset any
  timer that may be running when the strategy is returns a `:store`.


  # Usage
      defmodule MyApp.MyConsumer do
        alias RabbitMQStream.Consumer.OffsetTracking

        use RabbitMQStream.Consumer,
          offset_tracking: [OffsetTracking.IntervalStrategy, interval: 10_000]

        @impl true
        def handle_chunk(_chunk, _subscriber) do
          :ok
        end
      end


  # Parameters

  * `interval` - the time in milliseconds before storing the offset
  """

  def init(opts \\ []) do
    interval = Keyword.get(opts, :interval, 10_000)
    timer_ref = Process.send_after(self(), :run_offset_tracking, interval)

    {:erlang.monotonic_time(), interval, timer_ref}
  end

  def run({start, interval, timer_ref} = state, _) do
    now = :erlang.monotonic_time()

    if now - start >= interval do
      Process.cancel_timer(timer_ref)

      timer_ref = Process.send_after(self(), :run_offset_tracking, interval)
      {:store, {now, interval, timer_ref}}
    else
      {:skip, state}
    end
  end
end
