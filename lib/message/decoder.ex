defmodule RabbitMQStream.Message.Decoder do
  @moduledoc false

  alias RabbitMQStream.Message
  alias RabbitMQStream.Message.{Response, Request}

  alias RabbitMQStream.Message.Data.{
    TuneData,
    HeartbeatData,
    CloseData,
    MetadataUpdateData,
    QueryMetadataData,
    BrokerData,
    StreamData,
    PublishConfirmData,
    PublishErrorData,
    DeliverData
  }

  def decode!(<<key::integer-size(16), version::unsigned-integer-size(16), buffer::binary>>) do
    if Bitwise.band(key, 32768) > 0 do
      %Response{version: version, command: Message.Command.to_atom(Bitwise.band(key, 32767))}
    else
      %Request{version: version, command: Message.Command.to_atom(Bitwise.band(key, 32767))}
    end
    |> decode!(buffer)
  end

  def decode!(%Response{command: command} = response, buffer)
      when command in [
             :close,
             :create,
             :delete,
             :declare_publisher,
             :delete_publisher,
             :subscribe,
             :unsubscribe,
             :query_offset,
             :query_publisher_sequence,
             :peer_properties,
             :sasl_handshake,
             :sasl_authenticate,
             :tune,
             :open
           ] do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    %{
      response
      | data: Message.Data.decode_data(command, buffer),
        correlation_id: correlation_id,
        code: Message.Code.to_atom(code)
    }
  end

  def decode!(%Request{command: :close} = request, buffer) do
    <<correlation_id::unsigned-integer-size(32), buffer::binary>> = buffer

    <<code::unsigned-integer-size(16), buffer::binary>> = buffer

    {"", reason} = Message.Data.fetch_string(buffer)

    data = %CloseData{code: code, reason: reason}

    %{request | data: data, correlation_id: correlation_id}
  end

  def decode!(%Request{command: :tune} = request, buffer) do
    <<frame_max::unsigned-integer-size(32), heartbeat::unsigned-integer-size(32)>> = buffer

    data = %TuneData{frame_max: frame_max, heartbeat: heartbeat}

    %{request | data: data}
  end

  def decode!(%Request{command: :heartbeat} = request, "") do
    %{request | data: %HeartbeatData{}}
  end

  def decode!(%Request{command: :metadata_update} = request, buffer) do
    <<code::unsigned-integer-size(16), buffer::binary>> = buffer

    {"", stream_name} = Message.Data.fetch_string(buffer)

    data = %MetadataUpdateData{stream_name: stream_name}

    %{request | data: data, code: Message.Code.to_atom(code)}
  end

  def decode!(%Response{command: :query_metadata} = response, buffer) do
    <<correlation_id::unsigned-integer-size(32), buffer::binary>> = buffer

    {buffer, brokers} =
      Message.Data.decode_array(buffer, fn buffer, acc ->
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
      Message.Data.decode_array(buffer, fn buffer, acc ->
        <<
          size::integer-size(16),
          name::binary-size(size),
          code::unsigned-integer-size(16),
          leader::unsigned-integer-size(16),
          buffer::binary
        >> = buffer

        {buffer, replicas} =
          Message.Data.decode_array(buffer, fn buffer, acc ->
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

    data = %QueryMetadataData{brokers: brokers, streams: streams}

    %{response | correlation_id: correlation_id, data: data}
  end

  def decode!(%Request{command: :publish_confirm} = request, buffer) do
    <<publisher_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", publishing_ids} =
      Message.Data.decode_array(buffer, fn buffer, acc ->
        <<publishing_id::unsigned-integer-size(64), buffer::binary>> = buffer
        {buffer, [publishing_id] ++ acc}
      end)

    data = %PublishConfirmData{publisher_id: publisher_id, publishing_ids: publishing_ids}

    %{request | data: data}
  end

  def decode!(%Request{command: :publish_error} = request, buffer) do
    <<publisher_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", errors} =
      Message.Data.decode_array(buffer, fn buffer, acc ->
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

    data = %PublishErrorData{publisher_id: publisher_id, errors: errors}

    %{request | data: data}
  end

  def decode!(%Request{command: :deliver} = request, buffer) do
    <<subscription_id::unsigned-integer-size(8), rest::binary>> = buffer

    osiris_chunk =
      if rest != "" do
        Helpers.OsirisChunk.decode!(rest)
      else
        nil
      end

    data = %DeliverData{subscription_id: subscription_id, osiris_chunk: osiris_chunk}

    %{request | data: data}
  end
end
