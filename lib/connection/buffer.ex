defmodule RabbitMQStream.Connection.Buffer do
  alias RabbitMQStream.{Message, Connection}

  def decode!(buffer) when is_binary(buffer) do
    Stream.cycle([0])
    |> Enum.reduce_while({buffer, []}, fn
      _, {<<size::unsigned-integer-size(32), buffer::binary-size(size)>>, acc} ->
        {:halt, acc ++ [Message.Decoder.decode!(buffer)]}

      _, {<<size::unsigned-integer-size(32), buffer::binary-size(size), rest::binary>>, acc} ->
        {:cont, {rest, acc ++ [Message.Decoder.decode!(buffer)]}}
    end)
  end
end
