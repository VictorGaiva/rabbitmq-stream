defmodule RabbitStream.Message.Response do
  import RabbitStream.Helpers
  alias RabbitStream.Message.{Response, Command}
  alias RabbitStream.Connection
  require Logger

  alias Command.Code.{
    SaslHandshake,
    PeerProperties,
    SaslAuthenticate,
    Tune,
    Open,
    Close,
    Heartbeat
  }

  alias RabbitStream.Message.Data.{
    TuneData,
    SaslHandshakeData,
    SaslAuthenticateData,
    PeerPropertiesData,
    OpenData,
    HeartbeatData,
    CloseData
  }

  defmodule Code do
    match_codes(%{
      0x01 => :ok,
      0x02 => :stream_does_not_exist,
      0x03 => :subscription_id_already_exists,
      0x04 => :subscription_id_does_not_exist,
      0x05 => :stream_already_exists,
      0x06 => :stream_not_available,
      0x07 => :sasl_mechanism_not_supported,#
      0x08 => :authentication_failure,#
      0x09 => :sasl_error,#
      0x0A => :sasl_challenge,#
      0x0B => :sasl_authentication_failure_loopback,#
      0x0C => :virtual_host_access_failure,#
      0x0D => :unknown_frame,
      0x0E => :frame_too_large,
      0x0F => :internal_error,#
      0x10 => :access_refused,
      0x11 => :precondition_failed,
      0x12 => :publisher_does_not_exist,
      0x13 => :no_offset
    })
  end

  defstruct([
    :version,
    :command,
    :correlation_id,
    :data,
    :code
  ])

  defp fetch_string(<<size::integer-size(16), text::binary-size(size), rest::binary>>) do
    {rest, to_string(text)}
  end

  def decode!(%Response{command: %PeerProperties{}} = response, rest) do
    <<size::integer-size(32), buffer::binary>> = rest

    {"", peer_properties} =
      Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
        {buffer, key} = fetch_string(buffer)
        {buffer, value} = fetch_string(buffer)

        {buffer, [{key, value} | acc]}
      end)

    data = %PeerPropertiesData{
      peer_properties: peer_properties
    }

    %{response | data: data}
  end

  def decode!(%Response{command: %SaslHandshake{}} = response, rest) do
    <<size::integer-size(32), buffer::binary>> = rest

    {"", mechanisms} =
      Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
        {buffer, value} = fetch_string(buffer)
        {buffer, [value | acc]}
      end)

    data = %SaslHandshakeData{
      mechanisms: mechanisms
    }

    %{response | data: data}
  end

  def decode!(%Response{command: %SaslAuthenticate{}} = response, rest) do
    data = %SaslAuthenticateData{
      sasl_opaque_data: rest
    }

    %{response | data: data}
  end

  def decode!(%Response{command: %Tune{}} = response, rest) do
    <<
      frame_max::unsigned-integer-size(32),
      heartbeat::unsigned-integer-size(32)
    >> = rest

    data = %TuneData{
      frame_max: frame_max,
      heartbeat: heartbeat
    }

    %{response | data: data}
  end

  def decode!(%Response{command: %Open{}} = response, "") do
    data = %OpenData{connection_properties: []}

    %{response | data: data}
  end

  def decode!(%Response{command: %Open{}} = response, rest) do
    <<size::integer-size(32), buffer::binary>> = rest

    {"", connection_properties} =
      Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
        {buffer, key} = fetch_string(buffer)
        {buffer, value} = fetch_string(buffer)

        {buffer, [{key, value} | acc]}
      end)

    data = %OpenData{
      connection_properties: connection_properties
    }

    %{response | data: data}
  end

  def decode!(%Response{command: %Close{}} = response, "") do
    data = %CloseData{}

    %{response | data: data}
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

  def new!(%Connection{} = conn, :tune, correlation_id: correlation_id) do
    %Response{
      version: conn.version,
      command: %Tune{},
      correlation_id: correlation_id,
      data: %TuneData{
        frame_max: conn.frame_max,
        heartbeat: conn.heartbeat
      }
    }
  end

  def new!(%Connection{} = conn, :heartbeat, correlation_id: correlation_id) do
    %Response{
      version: conn.version,
      command: %Heartbeat{},
      correlation_id: correlation_id,
      data: %HeartbeatData{}
    }
  end

  def new!(%Connection{} = conn, :close,
        correlation_id: correlation_id,
        code: code
      ) do
    %Response{
      version: conn.version,
      correlation_id: correlation_id,
      command: %Close{},
      data: %CloseData{},
      code: code
    }
  end

  def new_encoded!(%Connection{} = conn, command, opts) do
    conn
    |> new!(command, opts)
    |> encode!()
  end
end
