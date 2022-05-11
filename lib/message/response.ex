defmodule XColony.Message.Response do
  import XColony.Helpers
  alias XColony.Message.{Response,Command}

  alias Command.Code.{
    SaslHandshake,
    PeerProperties,
    SaslAuthenticate,
    Tune
  }

  defmodule Code do
    match_codes( %{
      0x01 => :ok,
      0x02 => :stream_does_not_exist,
      0x03 => :subscription_id_already_exists,
      0x04 => :subscription_id_does_not_exist,
      0x05 => :stream_already_exists,
      0x06 => :stream_not_available,
      0x07 => :sasl_mechanism_not_supported,
      0x08 => :authentication_failure,
      0x09 => :sasl_error,
      0x0a => :sasl_challenge,
      0x0b => :sasl_authentication_failure_loopback,
      0x0c => :virtual_host_access_failure,
      0x0d => :unknown_frame,
      0x0e => :frame_too_large,
      0x0f => :internal_error,
      0x10 => :access_refused,
      0x11 => :precondition_failed,
      0x12 => :publisher_does_not_exist,
      0x13 => :no_offset,
    })
  end

  defstruct([
    :command,
    :version,
    :correlation_id,
    :response,
    :data
  ])

  defmodule TuneData do
    defstruct [
      :frame_max,
      :heartbeat
    ]
  end

  defmodule PeerPropertiesData do
    defstruct [
      :peer_properties,
    ]
  end

  defmodule SaslHandshakeData do
    defstruct [
      :mechanisms,
    ]
  end

  defp fetch_string(<<size::integer-size(16), text::binary-size(size), rest::binary>>) do
    {rest, to_string(text)}
  end


  def decode!(<<size::unsigned-integer-size(32), 0b1::1, key::bits-size(15), data::binary>>)  when byte_size(data) == size - 2 do
    <<version::unsigned-integer-size(16),correlation_id::unsigned-integer-size(32), response_code::unsigned-integer-size(16), rest::binary>> = data
    <<key::unsigned-integer-size(16)>> = <<0b0::1, key::bits>>

    %Response{
      version: version,
      correlation_id: correlation_id,
      command: Command.Code.decode(key),
      response: Code.decode(response_code)
    }
    |> decode!(rest)
  end

  def decode!(%Response{command: %PeerProperties{}} = response, rest) do
    <<size::integer-size(32), buffer::binary>> = rest

    {_, peer_properties} = Enum.reduce(0..(size-1), {buffer, []}, fn _, {buffer, acc} ->
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

    {_, mechanisms} = Enum.reduce(0..(size-1), {buffer, []}, fn _, {buffer, acc} ->
      {buffer, value} = fetch_string(buffer)
      {buffer, [value | acc]}
    end)

    data = %SaslHandshakeData{
      mechanisms: mechanisms
    }

    %{response | data: data}
  end

  def decode!(%Response{command: %SaslAuthenticate{}} = response, _rest) do
    response
  end

  def decode!(%Response{command: %Tune{}} = response, rest) do
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
end
