defmodule XColony.Message.Request do
  alias XColony.Connection

  alias XColony.Message.Command.Code.{
    PeerProperties,
    SaslHandshake,
    SaslAuthenticate,
    Tune
  }

  alias __MODULE__, as: Request

  defstruct [
    :version,
    :correlation_id,
    :command,
    :data,
  ]

  def encode_string(nil)  do
    <<-1::integer-size(16)>>
  end

  def encode_string(str)  do
    <<byte_size(str)::integer-size(16), str::binary>>
  end

  def encode_bytes(nil)  do
    <<-1::integer-size(32)>>
  end

  def encode_bytes(str)  do
    <<byte_size(str)::integer-size(32), str::binary>>
  end

  def encode_array(arr) do
    size = Enum.count(arr)
    arr = arr |> Enum.reduce(&<>/2)

    <<size::integer-size(32), arr::binary>>
  end


  def encode!(%Request{command: %PeerProperties{}} = request) do
    properties = request.data[:peer_properties]
      |> Enum.map(fn [key, value] -> encode_string(key) <> encode_string(value) end)
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
    mechanism = encode_string(request.data[:mechanism])
    credentials = encode_bytes(request.data[:credentials])

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      mechanism::binary,
      credentials::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def new!(:peer_properties, %Connection{} = conn) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation,
      command: %PeerProperties{},
      data: [
        peer_properties: [
          ["product","RabbitMQ Stream Client"],
          ["information","Development"],
          ["version","0.0.1"],
          ["platform","Elixir"],
        ]
      ]
    }
  end

  def new!(:sasl_handshake, %Connection{} = conn) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation,
      command: %SaslHandshake{},
      data: []
    }
  end

  def new!(:sasl_authenticate, %Connection{} = conn) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation,
      command: %SaslAuthenticate{},
      data: [
        mechanism: "PLAIN",
        credentials: "#{conn.username}:#{conn.password}"
      ]
    }
  end

  def new!(:tune, %Connection{frame_max: frame_max, heartbeat: heartbeat} = conn)
  when not is_nil(frame_max)
  when not is_nil(heartbeat)
  do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation,
      command: %Tune{},
      data: [
        frame_max: frame_max,
        heartbeat: heartbeat,
      ]
    }
  end
end
