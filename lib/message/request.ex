defmodule RabbitStream.Message.Request do
  alias RabbitStream.Connection

  alias RabbitStream.Message.Command.Code.{
    PeerProperties,
    SaslHandshake,
    SaslAuthenticate,
    Tune,
    Open,
    Heartbeat
  }

  alias RabbitStream.Message.Data.{
    TuneData,
    OpenData,
    PeerPropertiesData,
    SaslAuthenticateData,
    SaslHandshakeData,
    HeartbeatData
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

  def decode!(%Request{command: %Tune{}} = response, rest) do
    <<
      frame_max::unsigned-integer-size(32),
      heartbeat::unsigned-integer-size(32),
    >> = rest

    data = %TuneData{
      frame_max: frame_max,
      heartbeat: heartbeat
    }

    %{response | data: data}
  end

  def decode!(%Request{command: %Heartbeat{}} = response, "") do
    %{response | data: %HeartbeatData{}}
  end

  def encode!(%Request{command: %PeerProperties{}} = request) do
    properties = request.data.peer_properties
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

  def encode!(%Request{command: %SaslAuthenticate{}} = request)  do
    mechanism = encode_string(request.data.mechanism)
    credentials = encode_bytes("\u0000#{request.data.sasl_opaque_data[:username]}\u0000#{request.data.sasl_opaque_data[:password]}")

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
      request.version::unsigned-integer-size(16),
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def new!(%Connection{} = conn, :peer_properties) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation,
      command: %PeerProperties{},
      data: %PeerPropertiesData{
        peer_properties: [
          ["product","RabbitMQ Stream Client"],
          ["information","Development"],
          ["version","0.0.1"],
          ["platform","Elixir"],
        ]
      }
    }
  end

  def new!(%Connection{} = conn, :sasl_handshake) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation,
      command: %SaslHandshake{},
      data: %SaslHandshakeData{
        mechanisms: [
          # "PLAIN"
        ]
      }
    }
  end

  def new!(%Connection{} = conn, :sasl_authenticate) do
    cond do
      Enum.member?(conn.mechanisms, "PLAIN") ->
        %Request{
          version: conn.version,
          correlation_id: conn.correlation,
          command: %SaslAuthenticate{},
          data: %SaslAuthenticateData{
            mechanism: "PLAIN",
            sasl_opaque_data: [
              username: conn.username,
              password: conn.password,
            ]
          }
        }

      true -> raise "Unsupported SASL mechanism: #{conn.mechanisms}"
    end
  end

  def new!(%Connection{} = conn, :tune) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation,
      command: %Tune{},
      data: %TuneData{
        frame_max: conn.frame_max,
        heartbeat: conn.heartbeat,
      }
    }
  end

  def new!(%Connection{} = conn, :open) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation,
      command: %Open{},
      data: %OpenData{
        vhost: conn.vhost,
      }
    }
  end

  def new!(%Connection{} = conn, :heartbeat) do
    %Request{
      version: conn.version,
      command: %Heartbeat{},
      data: %HeartbeatData{}
    }
  end

  def new_encoded!(%Connection{} = conn, request) when is_atom(request) do
    conn
    |> new!(request)
    |> IO.inspect()
    |> encode!()
  end
end
