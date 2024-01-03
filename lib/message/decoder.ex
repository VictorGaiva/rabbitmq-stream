defmodule RabbitMQStream.Message.Decoder do
  @moduledoc false

  alias RabbitMQStream.Message.{Response, Request, Frame}

  alias RabbitMQStream.Message

  def parse(<<flag::1, key::bits-size(15), version::unsigned-integer-size(16), buffer::binary>>) do
    <<key::unsigned-integer-size(16)>> = <<0b0::1, key::bits>>
    command = Frame.code_to_command(key)

    case flag do
      0b1 ->
        %Response{version: version, command: command}

      0b0 ->
        %Request{version: version, command: command}
    end
    |> parse(buffer)
  end

  def parse(%Response{command: command} = response, buffer)
      when command in [
             :close,
             :create_stream,
             :delete_stream,
             :declare_publisher,
             :delete_publisher,
             :subscribe,
             :unsubscribe,
             :credit,
             :query_offset,
             :query_publisher_sequence,
             :peer_properties,
             :sasl_handshake,
             :sasl_authenticate,
             :open,
             :route,
             :partitions
           ] do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    %{
      response
      | data: Message.Data.decode_data(response, buffer),
        correlation_id: correlation_id,
        code: Frame.response_code_to_atom(code)
    }
  end

  def parse(%{command: command} = response, buffer)
      when command in [:close, :query_metadata] do
    <<correlation_id::unsigned-integer-size(32), buffer::binary>> = buffer

    %{response | data: Message.Data.decode_data(response, buffer), correlation_id: correlation_id}
  end

  def parse(%{command: command} = action, buffer)
      when command in [
             :tune,
             :heartbeat,
             :metadata_update,
             :publish_confirm,
             :publish_error,
             :deliver,
             :store_offset
           ] do
    %{action | data: Message.Data.decode_data(action, buffer)}
  end
end
