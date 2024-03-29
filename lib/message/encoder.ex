defmodule RabbitMQStream.Message.Encoder do
  @moduledoc false
  import RabbitMQStream.Message.Helpers

  alias RabbitMQStream.Message.Response
  alias RabbitMQStream.Message.Request
  alias RabbitMQStream.Message.Data

  def encode(command) do
    header = bake_header(command)

    payload = Data.encode(command)

    buffer = <<header::binary, payload::binary>>

    <<byte_size(buffer)::unsigned-integer-size(32), buffer::binary>>
  end

  defp bake_header(%Request{command: command, version: version, correlation_id: correlation_id})
       when command in [
              :peer_properties,
              :sasl_handshake,
              :sasl_authenticate,
              :open,
              :tune,
              :close,
              :create_stream,
              :delete_stream,
              :query_offset,
              :declare_producer,
              :delete_producer,
              :query_metadata,
              :query_producer_sequence,
              :subscribe,
              :unsubscribe,
              :route,
              :partitions,
              :exchange_command_versions,
              :stream_stats,
              :create_super_stream,
              :delete_super_stream
            ] do
    <<
      encode_command(command)::unsigned-integer-size(16),
      version::unsigned-integer-size(16),
      correlation_id::unsigned-integer-size(32)
    >>
  end

  defp bake_header(%Request{command: command, version: version})
       when command in [:heartbeat, :store_offset, :publish, :credit] do
    <<encode_command(command)::unsigned-integer-size(16), version::unsigned-integer-size(16)>>
  end

  defp bake_header(%Response{command: command, version: version, correlation_id: correlation_id, code: code})
       when command in [:close, :consumer_update] do
    <<
      0b1::1,
      encode_command(command)::unsigned-integer-size(15),
      version::unsigned-integer-size(16),
      correlation_id::unsigned-integer-size(32),
      encode_code(code)::unsigned-integer-size(16)
    >>
  end

  defp bake_header(%Response{command: command, version: version})
       when command in [:tune] do
    <<0b1::1, encode_command(command)::unsigned-integer-size(15), version::unsigned-integer-size(16)>>
  end
end
