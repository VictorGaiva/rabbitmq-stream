defmodule RabbitMQStream.Message do
  require Logger

  alias RabbitMQStream.Message.{Request, Response, Decoder}

  import RabbitMQStream.Helpers

  defmodule Code do
    @moduledoc false

    match_codes(%{
      0x01 => :ok,
      0x02 => :stream_does_not_exist,
      0x03 => :subscription_id_already_exists,
      0x04 => :subscription_id_does_not_exist,
      0x05 => :stream_already_exists,
      0x06 => :stream_not_available,
      0x07 => :sasl_mechanism_not_supported,
      0x08 => :authentication_failure,
      0x09 => :sasl_error,
      0x0A => :sasl_challenge,
      0x0B => :sasl_authentication_failure_loopback,
      0x0C => :virtual_host_access_failure,
      0x0D => :unknown_frame,
      0x0E => :frame_too_large,
      0x0F => :internal_error,
      0x10 => :access_refused,
      0x11 => :precondition_failed,
      0x12 => :publisher_does_not_exist,
      0x13 => :no_offset
    })
  end

  defmodule Command do
    @moduledoc false

    match_codes(%{
      # Client, Yes
      0x0001 => :declare_publisher,
      # Client, No
      0x0002 => :publish,
      # Server, No
      0x0003 => :publish_confirm,
      # Server, No
      0x0004 => :publish_error,
      # Client, Yes
      0x0005 => :query_publisher_sequence,
      # Client, Yes
      0x0006 => :delete_publisher,
      # Client, Yes
      0x0007 => :subscribe,
      # Server, No
      0x0008 => :deliver,
      # Client, No
      0x0009 => :credit,
      # Client, No
      0x000A => :store_offset,
      # Client, Yes
      0x000B => :query_offset,
      # Client, Yes
      0x000C => :unsubscribe,
      # Client, Yes
      0x000D => :create,
      # Client, Yes
      0x000E => :delete,
      # Client, Yes
      0x000F => :query_metadata,
      # Server, No
      0x0010 => :metadata_update,
      # Client, Yes
      0x0011 => :peer_properties,
      # Client, Yes
      0x0012 => :sasl_handshake,
      # Client, Yes
      0x0013 => :sasl_authenticate,
      # Server, Yes
      0x0014 => :tune,
      # Client, Yes
      0x0015 => :open,
      # Client & Server, Yes
      0x0016 => :close,
      # Client & Server, No
      0x0017 => :heartbeat
    })
  end

  def decode!(buffer) when is_binary(buffer) do
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
    command = Command.decode(key)

    case flag do
      0b1 ->
        %Response{version: version, command: command}

      0b0 ->
        %Request{version: version, command: command}
    end
    |> Decoder.decode!(buffer)
  end
end
