defmodule RabbitMQStream.Connection.Buffer do
  alias RabbitMQStream.{Message, Connection}
  alias Message.{Request, Response, Decoder, Command}
  import Bitwise

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
        {:halt, acc ++ [do_decode!(buffer)]}

      _, {<<size::unsigned-integer-size(32), buffer::binary-size(size), rest::binary>>, acc} ->
        {:cont, {rest, acc ++ [do_decode!(buffer)]}}
    end)
  end

  defp do_decode!(<<key::integer-size(16), version::unsigned-integer-size(16), buffer::binary>>) do
    if band(key, 32768) > 0 do
      %Response{version: version, command: Command.to_atom(band(key, 32767))}
    else
      %Request{version: version, command: Command.to_atom(band(key, 32767))}
    end
    |> Decoder.decode!(buffer)
  end
end
