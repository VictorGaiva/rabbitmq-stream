defmodule RabbitMQStream.Connection.Helpers do
  alias RabbitMQStream.Connection

  def push_request_tracker(%Connection{} = conn, type, from, data \\ nil) when is_atom(type) when is_pid(from) do
    request_tracker = Map.put(conn.request_tracker, {type, conn.correlation_sequence}, {from, data})

    %{conn | request_tracker: request_tracker}
  end

  def pop_request_tracker(%Connection{} = conn, type, correlation) when is_atom(type) do
    {entry, request_tracker} = Map.pop(conn.request_tracker, {type, correlation}, {nil, nil})

    {entry, %{conn | request_tracker: request_tracker}}
  end

  defguard is_offset(offset)
           when offset in [:first, :last, :next] or
                  (is_tuple(offset) and tuple_size(offset) == 2 and elem(offset, 0) in [:offset, :timestamp])
end
