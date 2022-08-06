defmodule RabbitMQStream.Message.Encoder do
  @moduledoc false

  alias RabbitMQStream.Message.{Response, Request}

  alias RabbitMQStream.Message.Command

  alias RabbitMQStream.Message.Data.{
    TuneData,
    CloseData,
    CreateData,
    DeleteData,
    StoreOffsetData,
    QueryOffsetData,
    DeclarePublisherData,
    DeletePublisherData,
    QueryMetadataData,
    QueryPublisherSequenceData,
    PublishData,
    SubscribeRequestData,
    UnsubscribeRequestData
  }

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

  def encode!(%Request{command: :peer_properties} = request) do
    properties =
      request.data.peer_properties
      |> Enum.map(fn {key, value} -> encode_string(key) <> encode_string(value) end)
      |> encode_array()

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      properties::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :sasl_handshake} = request) do
    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :sasl_authenticate} = request) do
    mechanism = encode_string(request.data.mechanism)

    credentials =
      encode_bytes("\u0000#{request.data.sasl_opaque_data[:username]}\u0000#{request.data.sasl_opaque_data[:password]}")

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      mechanism::binary,
      credentials::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :open} = request) do
    vhost = encode_string(request.data.vhost)

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      vhost::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :heartbeat} = request) do
    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :tune, data: %TuneData{} = data} = request) do
    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      data.frame_max::unsigned-integer-size(32),
      data.heartbeat::unsigned-integer-size(32)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :close, data: %CloseData{} = data} = request) do
    reason = encode_string(data.reason)

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      data.code::unsigned-integer-size(16),
      reason::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :create, data: %CreateData{} = data} = request) do
    stream_name = encode_string(data.stream_name)

    arguments =
      data.arguments
      |> Enum.map(fn {key, value} -> encode_string(Atom.to_string(key)) <> encode_string(value) end)
      |> encode_array()

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      stream_name::binary,
      arguments::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :delete, data: %DeleteData{} = data} = request) do
    stream_name = encode_string(data.stream_name)

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      stream_name::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :store_offset, data: %StoreOffsetData{} = data} = request) do
    offset_reference = encode_string(data.offset_reference)
    stream_name = encode_string(data.stream_name)

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      offset_reference::binary,
      stream_name::binary,
      data.offset::unsigned-integer-size(64)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :query_offset, data: %QueryOffsetData{} = data} = request) do
    offset_reference = encode_string(data.offset_reference)
    stream_name = encode_string(data.stream_name)

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      offset_reference::binary,
      stream_name::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :declare_publisher, data: %DeclarePublisherData{} = data} = request) do
    publisher_reference = encode_string(data.publisher_reference)
    stream_name = encode_string(data.stream_name)

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      data.id::unsigned-integer-size(8),
      publisher_reference::binary,
      stream_name::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :delete_publisher, data: %DeletePublisherData{} = data} = request) do
    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      data.publisher_id::unsigned-integer-size(8)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :query_metadata, data: %QueryMetadataData{} = data} = request) do
    streams =
      data.streams
      |> Enum.map(&encode_string/1)
      |> encode_array()

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      streams::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :query_publisher_sequence, data: %QueryPublisherSequenceData{} = data} = request) do
    publisher_reference = encode_string(data.publisher_reference)
    stream_name = encode_string(data.stream_name)

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      publisher_reference::binary,
      stream_name::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :publish, data: %PublishData{} = data} = request) do
    messages =
      encode_array(
        for {publishing_id, message} <- data.published_messages do
          <<
            publishing_id::unsigned-integer-size(64),
            encode_bytes(message)::binary
          >>
        end
      )

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      data.publisher_id::unsigned-integer-size(8),
      messages::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :subscribe, data: %SubscribeRequestData{} = data} = request) do
    stream_name = encode_string(data.stream_name)

    offset =
      case data.offset do
        :first ->
          <<1::unsigned-integer-size(16)>>

        :last ->
          <<2::unsigned-integer-size(16)>>

        :next ->
          <<3::unsigned-integer-size(16)>>

        {:offset, offset} ->
          <<4::unsigned-integer-size(16), offset::unsigned-integer-size(64)>>

        {:timestamp, timestamp} ->
          <<5::unsigned-integer-size(16), timestamp::integer-size(64)>>

        _ ->
          raise "Unknown offset type: #{data.offset}"
      end

    properties =
      encode_array(
        for {name, value} <- data.properties do
          <<
            encode_string(name)::binary,
            encode_bytes(value)::binary
          >>
        end
      )

    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      data.subscription_id::unsigned-integer-size(8),
      stream_name::binary,
      offset::binary,
      data.credit::unsigned-integer-size(16),
      properties::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: :unsubscribe, data: %UnsubscribeRequestData{} = data} = request) do
    data = <<
      Command.from_atom(request.command)::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      data.subscription_id::unsigned-integer-size(8)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Response{command: :tune, data: %TuneData{} = data} = response) do
    data = <<
      0b1::1,
      Command.from_atom(response.command)::unsigned-integer-size(15),
      response.version::unsigned-integer-size(16),
      data.frame_max::unsigned-integer-size(32),
      data.heartbeat::unsigned-integer-size(32)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Response{command: :heartbeat} = response) do
    data = <<
      0b1::1,
      Command.from_atom(response.command)::unsigned-integer-size(15),
      response.version::unsigned-integer-size(16)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Response{command: :close} = response) do
    data = <<
      0b1::1,
      Command.from_atom(response.command)::unsigned-integer-size(15),
      response.version::unsigned-integer-size(16),
      response.correlation_id::unsigned-integer-size(32),
      response.code::unsigned-integer-size(16)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end
end
