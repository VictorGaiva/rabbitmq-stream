defmodule RabbitMQStream.Connection.Buffer do
  alias RabbitMQStream.{Message, Connection}
  alias Message.{Request, Response, Decoder, Command}

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
      _, {<<size::unsigned-integer-size(32), _::binary-size(size)>> = buffer, acc} ->
        {:halt, acc ++ [do_decode!(buffer)]}

      _, {<<size::unsigned-integer-size(32), _::binary>> = buffer, acc} ->
        size = size + 4

        <<buffer::binary-size(size), rest::binary>> = buffer

        {:cont, {rest, acc ++ [do_decode!(buffer)]}}
    end)
  end

  defp do_decode!(<<size::unsigned-integer-size(32), buffer::binary-size(size)>>) do
    <<
      flag::1,
      key::bits-size(15),
      version::unsigned-integer-size(16),
      buffer::binary
    >> = buffer

    <<key::unsigned-integer-size(16)>> = <<0b0::1, key::bits>>
    command = Command.to_atom(key)

    case flag do
      0b1 ->
        %Response{version: version, command: command}

      0b0 ->
        %Request{version: version, command: command}
    end
    |> Decoder.decode!(buffer)
  end
end
