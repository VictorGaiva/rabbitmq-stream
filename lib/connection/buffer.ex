defmodule RabbitMQStream.Connection.Buffer do
  alias RabbitMQStream.{Message, Connection}

  def handle_decode(%Connection{} = conn, data) when is_binary(data) do
    messages = decode!(data)

    for message <- messages do
      send(self(), {:message, message})
    end

    conn
  end

  defp decode!(buffer) when is_binary(buffer) do
    Stream.cycle([0])
    |> Enum.reduce_while({buffer, []}, fn
      _, {<<size::unsigned-integer-size(32), buffer::binary-size(size)>>, acc} ->
        {:halt, acc ++ [Message.Decoder.decode!(buffer)]}

      _, {<<size::unsigned-integer-size(32), buffer::binary-size(size), rest::binary>>, acc} ->
        {:cont, {rest, acc ++ [Message.Decoder.decode!(buffer)]}}
    end)
  end
end
