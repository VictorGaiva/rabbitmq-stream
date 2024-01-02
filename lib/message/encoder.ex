defmodule RabbitMQStream.Message.Encoder do
  @moduledoc false

  alias RabbitMQStream.Message.{Response, Request, Frame}

  def encode!(command) do
    payload = encode_payload!(command)

    wrap(command, payload)
  end

  defp encode_payload!(%Request{command: :peer_properties, data: data}) do
    properties = encode_map(data.peer_properties)

    <<properties::binary>>
  end

  defp encode_payload!(%Request{command: :sasl_handshake}) do
    <<>>
  end

  defp encode_payload!(%Request{command: :sasl_authenticate, data: data}) do
    mechanism = encode_string(data.mechanism)

    credentials =
      encode_bytes("\u0000#{data.sasl_opaque_data[:username]}\u0000#{data.sasl_opaque_data[:password]}")

    <<mechanism::binary, credentials::binary>>
  end

  defp encode_payload!(%Request{command: :open, data: data}) do
    vhost = encode_string(data.vhost)

    <<vhost::binary>>
  end

  defp encode_payload!(%Request{command: :heartbeat}) do
    <<>>
  end

  defp encode_payload!(%Request{command: :tune, data: data}) do
    <<data.heartbeat::unsigned-integer-size(32)>>
  end

  defp encode_payload!(%Request{command: :close, data: data}) do
    reason = encode_string(data.reason)

    <<data.code::unsigned-integer-size(16), reason::binary>>
  end

  defp encode_payload!(%Request{command: :create_stream, data: data}) do
    stream_name = encode_string(data.stream_name)
    arguments = encode_map(data.arguments)

    <<stream_name::binary, arguments::binary>>
  end

  defp encode_payload!(%Request{command: :delete_stream, data: data}) do
    stream_name = encode_string(data.stream_name)

    <<stream_name::binary>>
  end

  defp encode_payload!(%Request{command: :store_offset, data: data}) do
    offset_reference = encode_string(data.offset_reference)
    stream_name = encode_string(data.stream_name)

    <<
      offset_reference::binary,
      stream_name::binary,
      data.offset::unsigned-integer-size(64)
    >>
  end

  defp encode_payload!(%Request{command: :query_offset, data: data}) do
    offset_reference = encode_string(data.offset_reference)
    stream_name = encode_string(data.stream_name)

    <<
      offset_reference::binary,
      stream_name::binary
    >>
  end

  defp encode_payload!(%Request{command: :declare_publisher, data: data}) do
    publisher_reference = encode_string(data.publisher_reference)
    stream_name = encode_string(data.stream_name)

    <<
      data.id::unsigned-integer-size(8),
      publisher_reference::binary,
      stream_name::binary
    >>
  end

  defp encode_payload!(%Request{command: :delete_publisher, data: data}) do
    <<data.publisher_id::unsigned-integer-size(8)>>
  end

  defp encode_payload!(%Request{command: :query_metadata, data: data}) do
    streams =
      data.streams
      |> Enum.map(&encode_string/1)
      |> encode_array()

    <<streams::binary>>
  end

  defp encode_payload!(%Request{command: :query_publisher_sequence, data: data}) do
    publisher_reference = encode_string(data.publisher_reference)
    stream_name = encode_string(data.stream_name)

    <<publisher_reference::binary, stream_name::binary>>
  end

  defp encode_payload!(%Request{command: :publish, data: data}) do
    messages =
      encode_array(
        for {publishing_id, message} <- data.published_messages do
          <<publishing_id::unsigned-integer-size(64), encode_bytes(message)::binary>>
        end
      )

    <<data.publisher_id::unsigned-integer-size(8), messages::binary>>
  end

  defp encode_payload!(%Request{command: :subscribe, data: data}) do
    stream_name = encode_string(data.stream_name)

    offset =
      case data.offset do
        :first -> <<1::unsigned-integer-size(16)>>
        :last -> <<2::unsigned-integer-size(16)>>
        :next -> <<3::unsigned-integer-size(16)>>
        {:offset, offset} -> <<4::unsigned-integer-size(16), offset::unsigned-integer-size(64)>>
        {:timestamp, timestamp} -> <<5::unsigned-integer-size(16), timestamp::integer-size(64)>>
      end

    properties = encode_map(data.properties)

    <<
      data.subscription_id::unsigned-integer-size(8),
      stream_name::binary,
      offset::binary,
      data.credit::unsigned-integer-size(16),
      properties::binary
    >>
  end

  defp encode_payload!(%Request{command: :unsubscribe, data: data}) do
    <<data.subscription_id::unsigned-integer-size(8)>>
  end

  defp encode_payload!(%Request{command: :credit, data: data}) do
    <<
      data.subscription_id::unsigned-integer-size(8),
      data.credit::unsigned-integer-size(16)
    >>
  end

  defp encode_payload!(%Response{command: :tune, data: data}) do
    <<
      data.frame_max::unsigned-integer-size(32),
      data.heartbeat::unsigned-integer-size(32)
    >>
  end

  defp encode_payload!(%Response{command: :close, code: code}) do
    <<
      Frame.atom_to_response_code(code)::unsigned-integer-size(16)
    >>
  end

  defp wrap(request, payload) do
    header = bake_header(request)

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
              :declare_publisher,
              :delete_publisher,
              :query_metadata,
              :query_publisher_sequence,
              :subscribe,
              :unsubscribe
            ] do
    <<
      Frame.command_to_code(command)::unsigned-integer-size(16),
      version::unsigned-integer-size(16),
      correlation_id::unsigned-integer-size(32)
    >>
  end

  defp bake_header(%Request{command: command, version: version})
       when command in [:heartbeat, :store_offset, :publish, :credit] do
    <<Frame.command_to_code(command)::unsigned-integer-size(16), version::unsigned-integer-size(16)>>
  end

  defp bake_header(%Response{command: command, version: version, correlation_id: correlation_id})
       when command in [:close] do
    <<
      0b1::1,
      Frame.command_to_code(command)::unsigned-integer-size(15),
      version::unsigned-integer-size(16),
      correlation_id::unsigned-integer-size(32)
    >>
  end

  defp bake_header(%Response{command: command, version: version})
       when command in [:tune] do
    <<0b1::1, Frame.command_to_code(command)::unsigned-integer-size(15), version::unsigned-integer-size(16)>>
  end

  defp encode_string(value) when is_atom(value) do
    encode_string(Atom.to_string(value))
  end

  defp encode_string(nil) do
    <<-1::integer-size(16)>>
  end

  defp encode_string(str) do
    <<byte_size(str)::integer-size(16), str::binary>>
  end

  defp encode_bytes(bytes) do
    <<byte_size(bytes)::integer-size(32), bytes::binary>>
  end

  defp encode_array([]) do
    <<0::integer-size(32)>>
  end

  defp encode_array(arr) do
    size = Enum.count(arr)
    arr = arr |> Enum.reduce(&<>/2)

    <<size::integer-size(32), arr::binary>>
  end

  defp encode_map(nil) do
    encode_array([])
  end

  defp encode_map(list) do
    list
    |> Enum.map(fn {key, value} -> encode_string(key) <> encode_string(value) end)
    |> encode_array()
  end
end
