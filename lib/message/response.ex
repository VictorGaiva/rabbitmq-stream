defmodule XColony.Message.Response do
  import XColony.Helpers

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
    :rest
  ])


  def decode(<<size::unsigned-integer-size(32), 0b1::1, key::bits-size(15), data::binary>>)  when byte_size(data) == size - 2 do
    <<version::unsigned-integer-size(16),correlation_id::unsigned-integer-size(32),response_code::unsigned-integer-size(16), rest::binary>> = data
    <<key::unsigned-integer-size(16)>> = <<0b0::1, key::bits>>

    %XColony.Message.Response{
      version: version,
      correlation_id: correlation_id,
      command: XColony.Message.Command.Code.decode(key),
      response: Code.decode(response_code),
      rest: rest
    }
  end


end
