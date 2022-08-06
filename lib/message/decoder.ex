defmodule RabbitMQStream.Message.Decoder do
  @moduledoc false

  alias RabbitMQStream.Message
  alias RabbitMQStream.Message.{Response, Request}

  alias RabbitMQStream.Message.Data.{
    TuneData,
    OpenData,
    PeerPropertiesData,
    SaslAuthenticateData,
    SaslHandshakeData,
    HeartbeatData,
    CloseData,
    CreateData,
    DeleteData,
    QueryOffsetData,
    MetadataUpdateData,
    DeclarePublisherData,
    DeletePublisherData,
    QueryMetadataData,
    BrokerData,
    StreamData,
    QueryPublisherSequenceData,
    PublishConfirmData,
    PublishErrorData,
    SubscribeResponseData,
    UnsubscribeResponseData,
    DeliverData
  }

  defp fetch_string(<<size::integer-size(16), text::binary-size(size), rest::binary>>) do
    {rest, to_string(text)}
  end

  defp decode_array("", _) do
    {"", []}
  end

  defp decode_array(<<0::integer-size(32), buffer::binary>>, _) do
    {buffer, []}
  end

  defp decode_array(<<size::integer-size(32), buffer::binary>>, foo) do
    Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
      foo.(buffer, acc)
    end)
  end

  def decode!(%Request{command: :tune} = response, buffer) do
    <<
      frame_max::unsigned-integer-size(32),
      heartbeat::unsigned-integer-size(32)
    >> = buffer

    data = %TuneData{
      frame_max: frame_max,
      heartbeat: heartbeat
    }

    %{response | data: data}
  end

  def decode!(%Request{command: :heartbeat} = response, "") do
    %{response | data: %HeartbeatData{}}
  end

  def decode!(%Request{command: :close} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), buffer::binary>> = buffer

    <<code::unsigned-integer-size(16), buffer::binary>> = buffer

    {"", reason} = fetch_string(buffer)

    data = %CloseData{
      code: code,
      reason: reason
    }

    %{response | data: data, correlation_id: correlation_id}
  end

  def decode!(%Request{command: :metadata_update} = response, buffer) do
    <<code::unsigned-integer-size(16), buffer::binary>> = buffer

    {"", stream_name} = fetch_string(buffer)

    data = %MetadataUpdateData{
      stream_name: stream_name
    }

    %{response | data: data, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :peer_properties} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    {"", peer_properties} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, key} = fetch_string(buffer)
        {buffer, value} = fetch_string(buffer)

        {buffer, [{key, value} | acc]}
      end)

    data = %PeerPropertiesData{
      peer_properties: peer_properties
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :sasl_handshake} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    {"", mechanisms} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, value} = fetch_string(buffer)
        {buffer, [value | acc]}
      end)

    data = %SaslHandshakeData{
      mechanisms: mechanisms
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :sasl_authenticate} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    data = %SaslAuthenticateData{
      sasl_opaque_data: buffer
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :tune} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    <<frame_max::unsigned-integer-size(32), heartbeat::unsigned-integer-size(32)>> = buffer

    data = %TuneData{
      frame_max: frame_max,
      heartbeat: heartbeat
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :open} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    connection_properties =
      if buffer != "" do
        {"", connection_properties} =
          decode_array(buffer, fn buffer, acc ->
            {buffer, key} = fetch_string(buffer)
            {buffer, value} = fetch_string(buffer)

            {buffer, [{key, value} | acc]}
          end)

        connection_properties
      else
        []
      end

    data = %OpenData{
      connection_properties: connection_properties
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :close} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer
    data = %CloseData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :create} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer

    data = %CreateData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :delete} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer
    data = %DeleteData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :query_offset} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer
    <<offset::unsigned-integer-size(64)>> = buffer

    data = %QueryOffsetData{
      offset: offset
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :declare_publisher} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer

    data = %DeclarePublisherData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :delete_publisher} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer

    data = %DeletePublisherData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :query_metadata} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), buffer::binary>> = buffer

    {buffer, brokers} =
      decode_array(buffer, fn buffer, acc ->
        <<reference::unsigned-integer-size(16), buffer::binary>> = buffer

        <<size::integer-size(16), host::binary-size(size), buffer::binary>> = buffer

        <<port::unsigned-integer-size(32), buffer::binary>> = buffer

        data = %BrokerData{
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

        data = %StreamData{
          code: code,
          name: name,
          leader: leader,
          replicas: replicas
        }

        {buffer, [data] ++ acc}
      end)

    data = %QueryMetadataData{
      brokers: brokers,
      streams: streams
    }

    %{response | correlation_id: correlation_id, data: data}
  end

  def decode!(%Response{command: :query_publisher_sequence} = response, buffer) do
    <<
      correlation_id::unsigned-integer-size(32),
      code::unsigned-integer-size(16),
      sequence::unsigned-integer-size(64)
    >> = buffer

    data = %QueryPublisherSequenceData{
      sequence: sequence
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Request{command: :publish_confirm} = response, buffer) do
    <<publisher_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", publishing_ids} =
      decode_array(buffer, fn buffer, acc ->
        <<publishing_id::unsigned-integer-size(64), buffer::binary>> = buffer
        {buffer, [publishing_id] ++ acc}
      end)

    data = %PublishConfirmData{
      publisher_id: publisher_id,
      publishing_ids: publishing_ids
    }

    %{response | data: data}
  end

  def decode!(%Request{command: :publish_error} = response, buffer) do
    <<publisher_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", errors} =
      decode_array(buffer, fn buffer, acc ->
        <<
          publishing_id::unsigned-integer-size(64),
          code::unsigned-integer-size(16),
          buffer::binary
        >> = buffer

        entry = %PublishErrorData.Error{
          code: Message.Code.to_atom(code),
          publishing_id: publishing_id
        }

        {buffer, [entry] ++ acc}
      end)

    data = %PublishErrorData{
      publisher_id: publisher_id,
      errors: errors
    }

    %{response | data: data}
  end

  def decode!(%Response{command: :subscribe} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer

    data = %SubscribeResponseData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :unsubscribe} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer

    data = %UnsubscribeResponseData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.to_atom(code)}
  end

  def decode!(%Request{command: :deliver} = request, buffer) do
    <<subscription_id::unsigned-integer-size(8), rest::binary>> = buffer

    osiris_chunk =
      if rest != "" do
        Helpers.OsirisChunk.decode!(rest)
      else
        nil
      end

    data = %DeliverData{
      subscription_id: subscription_id,
      osiris_chunk: osiris_chunk
    }

    %{request | data: data}
  end
end
