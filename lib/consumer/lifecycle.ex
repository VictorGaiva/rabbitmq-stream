defmodule RabbitMQStream.Consumer.LifeCycle do
  @moduledoc false
  alias RabbitMQStream.Message.Request
  alias RabbitMQStream.Consumer.{FlowControl, OffsetTracking}

  use GenServer
  require Logger

  @impl true
  def init(opts \\ []) do
    # Prevent startup if 'single_active_consumer' is active, but there is no
    # handle_update/2 callback defined.
    if Keyword.get(opts[:properties], :single_active_consumer) != nil do
      if not function_exported?(opts[:consumer_module], :handle_update, 2) do
        raise "handle_update/2 must be implemented when using single-active-consumer property"
      end
    end

    opts =
      opts
      |> Keyword.put(:credits, opts[:initial_credit])
      |> Keyword.put(:offset_tracking, OffsetTracking.init(opts[:offset_tracking], opts))
      |> Keyword.put(:flow_control, FlowControl.init(opts[:flow_control], opts))

    state = struct(RabbitMQStream.Consumer, opts)

    {:ok, state, {:continue, {:init, opts}}}
  end

  @impl true
  def handle_continue({:init, opts}, state) do
    last_offset =
      case state.connection.query_offset(state.stream_name, state.offset_reference) do
        {:ok, offset} ->
          {:offset, offset}

        _ ->
          opts[:initial_offset]
      end

    case state.connection.subscribe(state.stream_name, self(), last_offset, state.initial_credit, state.properties) do
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
    state.connection.unsubscribe(state.id)
    :ok
  end

  @impl true
  def handle_info({:chunk, %RabbitMQStream.OsirisChunk{} = chunk}, state) do
    chunk = RabbitMQStream.OsirisChunk.decode_messages!(chunk, state.serializer)

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

  def handle_info({:command, %Request{command: :consumer_update} = request}, state) do
    if function_exported?(state.consumer_module, :handle_update, 2) do
      case apply(state.consumer_module, :handle_update, [state, request.data.active]) do
        {:ok, offset} ->
          Logger.debug("Consumer upgraded to active consumer")
          state.connection.respond(request, offset: offset, code: :ok)
          {:noreply, state}

        {:error, reason} ->
          Logger.error("Error updating consumer: #{inspect(reason)}")
          state.connection.respond(request, code: :internal_error)

          {:noreply, state}
      end
    else
      Logger.error("handle_update/2 must be implemented when using single-active-consumer property")
      state.connection.respond(request, code: :internal_error)
      {:noreply, state}
    end
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
end
