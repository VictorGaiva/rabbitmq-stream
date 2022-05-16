defmodule RabbitStream.Message.Encoder do
  alias RabbitStream.Message.{Response, Request}

  alias RabbitStream.Message.Command.{
    PeerProperties,
    SaslHandshake,
    SaslAuthenticate,
    Tune,
    Open,
    Heartbeat,
    Close,
    Create,
    Delete,
    StoreOffset,
    QueryOffset
  }

  alias RabbitStream.Message.Data.{
    TuneData,
    CloseData,
    CreateData,
    DeleteData,
    StoreOffsetData,
    QueryOffsetData
  }

  def encode_string(nil) do
    <<-1::integer-size(16)>>
  end

  def encode_string(str) do
    <<byte_size(str)::integer-size(16), str::binary>>
  end

  def encode_bytes(nil) do
    <<-1::integer-size(32)>>
  end

  def encode_bytes(bytes) do
    <<byte_size(bytes)::integer-size(32), bytes::binary>>
  end

  def encode_array([]) do
    <<0::integer-size(32)>>
  end

  def encode_array(arr) do
    size = Enum.count(arr)
    arr = arr |> Enum.reduce(&<>/2)

    <<size::integer-size(32), arr::binary>>
  end

  def encode!(%Request{command: %PeerProperties{}} = request) do
    properties =
      request.data.peer_properties
      |> Enum.map(fn {key, value} -> encode_string(key) <> encode_string(value) end)
      |> encode_array()

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      properties::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %SaslHandshake{}} = request) do
    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %SaslAuthenticate{}} = request) do
    mechanism = encode_string(request.data.mechanism)

    credentials =
      encode_bytes("\u0000#{request.data.sasl_opaque_data[:username]}\u0000#{request.data.sasl_opaque_data[:password]}")

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      mechanism::binary,
      credentials::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %Open{}} = request) do
    vhost = encode_string(request.data.vhost)

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      vhost::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %Heartbeat{}} = request) do
    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %Tune{}, data: %TuneData{} = data} = request) do
    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      data.frame_max::unsigned-integer-size(32),
      data.heartbeat::unsigned-integer-size(32)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %Close{}, data: %CloseData{} = data} = request) do
    reason = encode_string(data.reason)

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      data.code::unsigned-integer-size(16),
      reason::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %Create{}, data: %CreateData{} = data} = request) do
    stream_name = encode_string(data.stream_name)

    arguments =
      data.arguments
      |> Enum.map(fn {key, value} -> encode_string(key) <> encode_string(value) end)
      |> encode_array()

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      stream_name::binary,
      arguments::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %Delete{}, data: %DeleteData{} = data} = request) do
    stream_name = encode_string(data.stream_name)

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      stream_name::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %StoreOffset{}, data: %StoreOffsetData{} = data} = request) do
    reference = encode_string(data.reference)
    stream_name = encode_string(data.stream_name)

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      reference::binary,
      stream_name::binary,
      data.offset::unsigned-integer-size(64)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %QueryOffset{}, data: %QueryOffsetData{} = data} = request) do
    reference = encode_string(data.reference)
    stream_name = encode_string(data.stream_name)

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      reference::binary,
      stream_name::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Response{command: %Tune{}, data: %TuneData{} = data} = response) do
    data = <<
      0b1::1,
      response.command.code::unsigned-integer-size(15),
      response.version::unsigned-integer-size(16),
      data.frame_max::unsigned-integer-size(32),
      data.heartbeat::unsigned-integer-size(32)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Response{command: %Heartbeat{}} = response) do
    data = <<
      0b1::1,
      response.command.code::unsigned-integer-size(15),
      response.version::unsigned-integer-size(16)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Response{command: %Close{}} = response) do
    data = <<
      0b1::1,
      response.command.code::unsigned-integer-size(15),
      response.version::unsigned-integer-size(16),
      response.correlation_id::unsigned-integer-size(32),
      response.code.code::unsigned-integer-size(16)
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end
end
