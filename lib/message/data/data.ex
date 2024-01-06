defmodule RabbitMQStream.Message.Data do
  alias RabbitMQStream.Message.Data.Types
  alias RabbitMQStream.Message.{Response, Request}
  import RabbitMQStream.Message.Helpers

  def decode(%{command: :heartbeat}, ""), do: %Types.HeartbeatData{}
  def decode(%Response{command: :close}, ""), do: %Types.CloseData{}

  def decode(%Response{command: :create_stream}, ""), do: %Types.CreateStreamResponseData{}
  def decode(%Response{command: :delete_stream}, ""), do: %Types.DeleteStreamResponseData{}
  def decode(%Response{command: :declare_publisher}, ""), do: %Types.DeclarePublisherResponseData{}
  def decode(%Response{command: :delete_publisher}, ""), do: %Types.DeletePublisherResponseData{}
  def decode(%Response{command: :subscribe}, ""), do: %Types.SubscribeResponseData{}
  def decode(%Response{command: :unsubscribe}, ""), do: %Types.UnsubscribeResponseData{}
  def decode(%Response{command: :credit}, ""), do: %Types.CreditResponseData{}
  def decode(%Response{command: :store_offset}, ""), do: %Types.StoreOffsetResponseData{}

  def decode(%Request{command: :publish_confirm}, buffer) do
    <<publisher_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", publishing_ids} =
      decode_array(buffer, fn buffer, acc ->
        <<publishing_id::unsigned-integer-size(64), buffer::binary>> = buffer
        {buffer, [publishing_id] ++ acc}
      end)

    %Types.PublishConfirmData{publisher_id: publisher_id, publishing_ids: publishing_ids}
  end

  def decode(%Response{command: :publish_error}, buffer) do
    <<publisher_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", errors} =
      decode_array(buffer, fn buffer, acc ->
        <<
          publishing_id::unsigned-integer-size(64),
          code::unsigned-integer-size(16),
          buffer::binary
        >> = buffer

        entry = %Types.PublishErrorData.Error{
          code: decode_code(code),
          publishing_id: publishing_id
        }

        {buffer, [entry] ++ acc}
      end)

    %Types.PublishErrorData{publisher_id: publisher_id, errors: errors}
  end

  def decode(%Request{version: 1, command: :deliver}, buffer) do
    <<subscription_id::unsigned-integer-size(8), rest::binary>> = buffer

    osiris_chunk = RabbitMQStream.OsirisChunk.decode!(rest)

    %Types.DeliverData{subscription_id: subscription_id, osiris_chunk: osiris_chunk}
  end

  def decode(%Request{version: 2, command: :deliver}, buffer) do
    <<subscription_id::unsigned-integer-size(8), committed_offset::unsigned-integer-size(64), rest::binary>> = buffer

    osiris_chunk = RabbitMQStream.OsirisChunk.decode!(rest)

    %Types.DeliverData{
      subscription_id: subscription_id,
      committed_offset: committed_offset,
      osiris_chunk: osiris_chunk
    }
  end

  def decode(%{command: :query_metadata}, buffer) do
    {buffer, brokers} =
      decode_array(buffer, fn buffer, acc ->
        <<reference::unsigned-integer-size(16), buffer::binary>> = buffer

        <<size::integer-size(16), host::binary-size(size), buffer::binary>> = buffer

        <<port::unsigned-integer-size(32), buffer::binary>> = buffer

        data = %Types.BrokerData{
          reference: reference,
          host: host,
          port: port
        }

        {buffer, [data] ++ acc}
      end)

    {"", streams} =
      decode_array(buffer, fn buffer, acc ->
        <<
          size::integer-size(16),
          name::binary-size(size),
          code::unsigned-integer-size(16),
          leader::unsigned-integer-size(16),
          buffer::binary
        >> = buffer

        {buffer, replicas} =
          decode_array(buffer, fn buffer, acc ->
            <<replica::unsigned-integer-size(16), buffer::binary>> = buffer

            {buffer, [replica] ++ acc}
          end)

        data = %Types.StreamData{
          code: code,
          name: name,
          leader: leader,
          replicas: replicas
        }

        {buffer, [data] ++ acc}
      end)

    %Types.QueryMetadataResponseData{brokers: brokers, streams: streams}
  end

  def decode(%Request{command: :close}, <<code::unsigned-integer-size(16), buffer::binary>>) do
    {"", reason} = decode_string(buffer)

    %Types.CloseData{code: code, reason: reason}
  end

  def decode(%{command: :metadata_update}, <<code::unsigned-integer-size(16), buffer::binary>>) do
    {"", stream_name} = decode_string(buffer)

    %Types.MetadataUpdateData{stream_name: stream_name, code: code}
  end

  def decode(%{command: :query_offset}, <<offset::unsigned-integer-size(64)>>) do
    %Types.QueryOffsetResponseData{offset: offset}
  end

  def decode(%Response{command: :query_publisher_sequence}, <<sequence::unsigned-integer-size(64)>>) do
    %Types.QueryPublisherSequenceData{sequence: sequence}
  end

  def decode(%{command: :peer_properties}, buffer) do
    {"", peer_properties} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, key} = decode_string(buffer)
        {buffer, value} = decode_string(buffer)

        {buffer, [{key, value} | acc]}
      end)

    %Types.PeerPropertiesData{peer_properties: peer_properties}
  end

  def decode(%{command: :sasl_handshake}, buffer) do
    {"", mechanisms} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, value} = decode_string(buffer)
        {buffer, [value | acc]}
      end)

    %Types.SaslHandshakeData{mechanisms: mechanisms}
  end

  def decode(%{command: :sasl_authenticate}, buffer) do
    %Types.SaslAuthenticateData{sasl_opaque_data: buffer}
  end

  def decode(%{command: :tune}, <<frame_max::unsigned-integer-size(32), heartbeat::unsigned-integer-size(32)>>) do
    %Types.TuneData{frame_max: frame_max, heartbeat: heartbeat}
  end

  def decode(%{command: :open}, buffer) do
    connection_properties =
      if buffer != "" do
        {"", connection_properties} =
          decode_array(buffer, fn buffer, acc ->
            {buffer, key} = decode_string(buffer)
            {buffer, value} = decode_string(buffer)

            {buffer, [{key, value} | acc]}
          end)

        connection_properties
      else
        []
      end

    %Types.OpenData{connection_properties: connection_properties}
  end

  def decode(%Response{command: :route}, buffer) do
    {"", stream} = decode_string(buffer)

    %Types.RouteResponseData{stream: stream}
  end

  def decode(%Response{command: :partitions}, buffer) do
    {"", stream} = decode_string(buffer)

    %Types.RouteResponseData{stream: stream}
  end

  def encode(%Response{command: :close, code: code}) do
    <<
      encode_code(code)::unsigned-integer-size(16)
    >>
  end

  def encode(%Request{command: :peer_properties, data: data}) do
    properties = encode_map(data.peer_properties)

    <<properties::binary>>
  end

  def encode(%Request{command: :sasl_handshake}) do
    <<>>
  end

  def encode(%Request{command: :sasl_authenticate, data: data}) do
    mechanism = encode_string(data.mechanism)

    credentials =
      encode_bytes("\u0000#{data.sasl_opaque_data[:username]}\u0000#{data.sasl_opaque_data[:password]}")

    <<mechanism::binary, credentials::binary>>
  end

  def encode(%Request{command: :open, data: data}) do
    vhost = encode_string(data.vhost)

    <<vhost::binary>>
  end

  def encode(%Request{command: :heartbeat}) do
    <<>>
  end

  def encode(%Request{command: :tune, data: data}) do
    <<data.heartbeat::unsigned-integer-size(32)>>
  end

  def encode(%Request{command: :close, data: data}) do
    reason = encode_string(data.reason)

    <<data.code::unsigned-integer-size(16), reason::binary>>
  end

  def encode(%Request{command: :create_stream, data: data}) do
    stream_name = encode_string(data.stream_name)
    arguments = encode_map(data.arguments)

    <<stream_name::binary, arguments::binary>>
  end

  def encode(%Request{command: :delete_stream, data: data}) do
    stream_name = encode_string(data.stream_name)

    <<stream_name::binary>>
  end

  def encode(%Request{command: :store_offset, data: data}) do
    offset_reference = encode_string(data.offset_reference)
    stream_name = encode_string(data.stream_name)

    <<
      offset_reference::binary,
      stream_name::binary,
      data.offset::unsigned-integer-size(64)
    >>
  end

  def encode(%Request{command: :query_offset, data: data}) do
    offset_reference = encode_string(data.offset_reference)
    stream_name = encode_string(data.stream_name)

    <<
      offset_reference::binary,
      stream_name::binary
    >>
  end

  def encode(%Request{command: :declare_publisher, data: data}) do
    publisher_reference = encode_string(data.publisher_reference)
    stream_name = encode_string(data.stream_name)

    <<
      data.id::unsigned-integer-size(8),
      publisher_reference::binary,
      stream_name::binary
    >>
  end

  def encode(%Request{command: :delete_publisher, data: data}) do
    <<data.publisher_id::unsigned-integer-size(8)>>
  end

  def encode(%Request{command: :query_metadata, data: data}) do
    streams =
      data.streams
      |> Enum.map(&encode_string/1)
      |> encode_array()

    <<streams::binary>>
  end

  def encode(%Request{command: :query_publisher_sequence, data: data}) do
    publisher_reference = encode_string(data.publisher_reference)
    stream_name = encode_string(data.stream_name)

    <<publisher_reference::binary, stream_name::binary>>
  end

  def encode(%Request{version: 1, command: :publish, data: data}) do
    messages =
      encode_array(
        for {publishing_id, message} <- data.published_messages do
          <<publishing_id::unsigned-integer-size(64), encode_bytes(message)::binary>>
        end
      )

    <<data.publisher_id::unsigned-integer-size(8), messages::binary>>
  end

  def encode(%Request{version: 2, command: :publish, data: data}) do
    # filter_value = encode_string(data.filter_value)

    messages =
      encode_array(
        for {publishing_id, message} <- data.published_messages do
          <<
            publishing_id::unsigned-integer-size(64),
            # filter_value::binary,
            encode_bytes(message)::binary
          >>
        end
      )

    <<data.publisher_id::unsigned-integer-size(8), messages::binary>>
  end

  def encode(%Request{command: :subscribe, data: data}) do
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

  def encode(%Request{command: :unsubscribe, data: data}) do
    <<data.subscription_id::unsigned-integer-size(8)>>
  end

  def encode(%Request{command: :credit, data: data}) do
    <<
      data.subscription_id::unsigned-integer-size(8),
      data.credit::unsigned-integer-size(16)
    >>
  end

  def encode(%Request{command: :route, data: data}) do
    routing_key = encode_string(data.routing_key)
    super_stream = encode_string(data.super_stream)

    <<routing_key::binary, super_stream::binary>>
  end

  def encode(%Request{command: :partitions, data: data}) do
    super_stream = encode_string(data.super_stream)

    <<super_stream::binary>>
  end

  def encode(%Response{command: :tune, data: data}) do
    <<
      data.frame_max::unsigned-integer-size(32),
      data.heartbeat::unsigned-integer-size(32)
    >>
  end
end