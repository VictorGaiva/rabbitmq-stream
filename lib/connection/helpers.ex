defmodule RabbitMQStream.Connection.Helpers do
  def push_tracker(conn, type, from, data \\ nil) when is_atom(type) when is_pid(from) do
    request_tracker = Map.put(conn.request_tracker, {type, conn.correlation_sequence}, {from, data})

    %{conn | request_tracker: request_tracker}
  end

  def pop_tracker(conn, type, correlation) when is_atom(type) do
    {entry, request_tracker} = Map.pop(conn.request_tracker, {type, correlation}, {nil, nil})

    {entry, %{conn | request_tracker: request_tracker}}
  end

  def push(conn, action, command, opts \\ []) do
    commands_buffer = :queue.in({action, command, opts}, conn.commands_buffer)

    %{conn | commands_buffer: commands_buffer}
  end

  defguard is_offset(offset)
           when offset in [:first, :last, :next] or
                  (is_tuple(offset) and tuple_size(offset) == 2 and elem(offset, 0) in [:offset, :timestamp])
end
