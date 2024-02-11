defmodule RabbitMQStream.Consumer.LifeCycle do
  @moduledoc false
  alias RabbitMQStream.Message.Request
  alias RabbitMQStream.Consumer.{FlowControl, OffsetTracking}

  use GenServer
  require Logger

  @impl true
  def init(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:initial_credit, 50_000)
      |> Keyword.put_new(:offset_tracking, count: [store_after: 50])
      |> Keyword.put_new(:flow_control, count: [credit_after: {:count, 1}])
      |> Keyword.put_new(:offset_reference, Atom.to_string(opts[:consumer_module]))
      |> Keyword.put_new(:properties, [])

    unless opts[:initial_offset] != nil do
      raise "initial_offset is required"
    end

    # Prevent startup if 'single_active_consumer' is active, but there is no
    # handle_update/2 callback defined.
    if Keyword.get(opts[:properties], :single_active_consumer) != nil do
      if not function_exported?(opts[:consumer_module], :handle_update, 2) do
        raise "handle_update/2 must be implemented when using single-active-consumer property"
      end
    end

    opts =
      opts
      |> Keyword.put(:offset_tracking, OffsetTracking.init(opts[:offset_tracking], opts))
      |> Keyword.put(:flow_control, FlowControl.init(opts[:flow_control], opts))
      |> Keyword.put(:credits, opts[:initial_credit])

    state = struct(RabbitMQStream.Consumer, opts)

    {:ok, state, {:continue, {:init, opts}}}
  end

  @impl true
  def handle_continue({:init, opts}, state) do
    state = apply(state.consumer_module, :before_start, [opts, state])

    last_offset =
      case RabbitMQStream.Connection.query_offset(state.connection, state.stream_name, state.offset_reference) do
        {:ok, offset} ->
          {:offset, offset}

        _ ->
          opts[:initial_offset]
      end

    case RabbitMQStream.Connection.subscribe(
           state.connection,
           state.stream_name,
           self(),
           last_offset,
           state.initial_credit,
           state.properties
         ) do
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
  def terminate(_reason, %{id: nil}), do: :ok

  def terminate(_reason, state) do
    # While not guaranteed, we attempt to store the offset when terminating. Useful for when performing
    # upgrades, and in a 'single-active-consumer' scenario.
    if state.last_offset != nil do
      RabbitMQStream.Connection.store_offset(
        state.connection,
        state.stream_name,
        state.offset_reference,
        state.last_offset
      )
    end

    RabbitMQStream.Connection.unsubscribe(state.connection, state.id)
    :ok
  end

  @impl true
  def handle_info({:chunk, %RabbitMQStream.OsirisChunk{} = chunk}, state) do
    # TODO: Possibly add 'filter_value', as described as necessary in the documentation.
    chunk = RabbitMQStream.OsirisChunk.decode_messages!(chunk, state.consumer_module)

    cond do
      function_exported?(state.consumer_module, :handle_chunk, 1) ->
        apply(state.consumer_module, :handle_chunk, [chunk])

      function_exported?(state.consumer_module, :handle_chunk, 2) ->
        apply(state.consumer_module, :handle_chunk, [chunk, state])

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

    state =
      %{
        state
        | offset_tracking: offset_tracking,
          last_offset: chunk.chunk_id,
          credits: state.credits - chunk.num_entries
      }

    state = state |> OffsetTracking.run() |> FlowControl.run()

    {:noreply, state}
  end

  def handle_info(:run_offset_tracking, state) do
    {:noreply, OffsetTracking.run(state)}
  end

  def handle_info(:run_flow_control, state) do
    {:noreply, FlowControl.run(state)}
  end

  def handle_info({:command, %Request{command: :consumer_update, data: data} = request}, state) do
    if function_exported?(state.consumer_module, :handle_update, 2) do
      action = if data.active, do: :upgrade, else: :downgrade

      # It seems that sending an offset to the server when a consumer is being
      # downgraded from 'active' to 'inactive' has no effect. But the server
      # still waits a response. A consumer module might use this downgrade
      # to send the current offset to the consumer that is being upgraded.

      case apply(state.consumer_module, :handle_update, [state, action]) do
        {:ok, offset} ->
          Logger.debug("Consumer upgraded to active consumer")
          RabbitMQStream.Connection.respond(state.connection, request, offset: offset, code: :ok)

        {:error, reason} ->
          Logger.error("Error updating consumer: #{inspect(reason)}")
          RabbitMQStream.Connection.respond(state.connection, request, code: :internal_error)
      end
    else
      Logger.error("handle_update/2 must be implemented when using single-active-consumer property")
      RabbitMQStream.Connection.respond(state.connection, request, code: :internal_error)
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:credit, amount}, state) do
    RabbitMQStream.Connection.credit(state.connection, state.id, amount)
    {:noreply, %{state | credits: state.credits + amount}}
  end

  def handle_cast(:store_offset, state) do
    RabbitMQStream.Connection.store_offset(
      state.connection,
      state.stream_name,
      state.offset_reference,
      state.last_offset
    )

    {:noreply, state}
  end

  @impl true
  def handle_call(:get_credits, _from, state) do
    {:reply, state.credits, state}
  end
end
