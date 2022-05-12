defmodule RabbitStream.Message do
  require Logger

  alias RabbitStream.Message.{Request, Response, Command}

  alias Command.Code.{
    SaslHandshake,
    PeerProperties,
    SaslAuthenticate,
    # Tune,
    DeclarePublisher,
    QueryPublisherSequence,
    DeclarePublisher,
    # Subscribe,
    QueryOffset,
    # Unsubscribe,
    # Create,
    # Delete,
    # Metadata,
    PeerProperties,
    SaslHandshake,
    SaslAuthenticate,
    Open,
    # Heartbeat,
    Close
  }

  defp has_correlation?(command) do
    command in [
      %DeclarePublisher{},
      %QueryPublisherSequence{},
      %DeclarePublisher{},
      %QueryOffset{},
      %PeerProperties{},
      %SaslHandshake{},
      %SaslAuthenticate{},
      %Open{},
      %Close{}
    ]
  end

  # def decode!(<<size::unsigned-integer-size(32), buffer::binary-size(size)>>) do
  def decode!(<<_::unsigned-integer-size(32), buffer::binary>>) do
    <<
      flag::1,
      key::bits-size(15),
      version::unsigned-integer-size(16),
      buffer::binary
    >> = buffer

    <<key::unsigned-integer-size(16)>> = <<0b0::1, key::bits>>
    command = Command.Code.decode(key)

    message =
      case {flag, has_correlation?(command)} do
        {0b1, true} ->
          <<correlation_id::unsigned-integer-size(32), response_code::unsigned-integer-size(16),
            buffer::binary>> = buffer

          %Response{
            version: version,
            command: command,
            correlation_id: correlation_id,
            response_code: Response.Code.decode(response_code)
          }
          |> Response.decode!(buffer)

        {0b0, true} ->
          <<correlation_id::unsigned-integer-size(32), buffer::binary>> = buffer

          %Request{version: version, command: command, correlation_id: correlation_id}
          |> Request.decode!(buffer)

        {0b1, false} ->
          %Response{command: command, version: version}
          |> Response.decode!(buffer)

        {0b0, false} ->
          %Request{command: command, version: version}
          |> Request.decode!(buffer)
      end

    Logger.debug(message)

    message
  end
end
