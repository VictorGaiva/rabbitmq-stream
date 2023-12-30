defmodule RabbitMQStream.Connection.Helpers do
  defguard is_offset(offset)
           when offset in [:first, :last, :next] or
                  (is_tuple(offset) and tuple_size(offset) == 2 and elem(offset, 0) in [:offset, :timestamp])
end
