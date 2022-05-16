defmodule RabbitStream.Message.Decoder do
  alias RabbitStream.Message
  alias RabbitStream.Message.{Response, Request}

  alias RabbitStream.Message.Command.Code.{
    PeerProperties,
    SaslHandshake,
    SaslAuthenticate,
    Tune,
    Open,
    Heartbeat,
    Close,
    Create,
    Delete,
    QueryOffset,
    MetadataUpdate
  }

  alias RabbitStream.Message.Data.{
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
    MetadataUpdateData
  }

  defp fetch_string(<<size::integer-size(16), text::binary-size(size), rest::binary>>) do
    {rest, to_string(text)}
  end

  def decode!(%Request{command: %Tune{}} = response, buffer) do
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

  def decode!(%Request{command: %Heartbeat{}} = response, "") do
    %{response | data: %HeartbeatData{}}
  end

  def decode!(%Request{command: %Close{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), buffer::binary>> = buffer

    <<code::unsigned-integer-size(16), buffer::binary>> = buffer

    {"", reason} = fetch_string(buffer)

    data = %CloseData{
      code: code,
      reason: reason
    }

    %{response | data: data, correlation_id: correlation_id}
  end

  def decode!(%Request{command: %MetadataUpdate{}} = response, buffer) do
    <<code::unsigned-integer-size(16), buffer::binary>> = buffer

    {"", stream_name} = fetch_string(buffer)

    data = %MetadataUpdateData{
      stream_name: stream_name
    }

    %{response | data: data, code: Message.Code.decode(code)}
  end

  def decode!(%Response{command: %PeerProperties{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    <<size::integer-size(32), buffer::binary>> = buffer

    {"", peer_properties} =
      Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
        {buffer, key} = fetch_string(buffer)
        {buffer, value} = fetch_string(buffer)

        {buffer, [{key, value} | acc]}
      end)

    data = %PeerPropertiesData{
      peer_properties: peer_properties
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.decode(code)}
  end

  def decode!(%Response{command: %SaslHandshake{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    <<size::integer-size(32), buffer::binary>> = buffer

    {"", mechanisms} =
      Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
        {buffer, value} = fetch_string(buffer)
        {buffer, [value | acc]}
      end)

    data = %SaslHandshakeData{
      mechanisms: mechanisms
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.decode(code)}
  end

  def decode!(%Response{command: %SaslAuthenticate{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    data = %SaslAuthenticateData{
      sasl_opaque_data: buffer
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.decode(code)}
  end

  def decode!(%Response{command: %Tune{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    <<frame_max::unsigned-integer-size(32), heartbeat::unsigned-integer-size(32)>> = buffer

    data = %TuneData{
      frame_max: frame_max,
      heartbeat: heartbeat
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.decode(code)}
  end

  def decode!(%Response{command: %Open{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    connection_properties =
      if buffer != "" do
        <<size::integer-size(32), buffer::binary>> = buffer

        {"", connection_properties} =
          Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
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

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.decode(code)}
  end

  def decode!(%Response{command: %Close{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer
    data = %CloseData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.decode(code)}
  end

  def decode!(%Response{command: %Create{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer

    data = %CreateData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.decode(code)}
  end

  def decode!(%Response{command: %Delete{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16)>> = buffer
    data = %DeleteData{}

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.decode(code)}
  end

  def decode!(%Response{command: %QueryOffset{}} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer
    <<offset::unsigned-integer-size(64)>> = buffer

    data = %QueryOffsetData{
      offset: offset
    }

    %{response | data: data, correlation_id: correlation_id, code: Message.Code.decode(code)}
  end
end
