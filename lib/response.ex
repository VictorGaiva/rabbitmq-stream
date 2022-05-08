defmodule XColony.Response do
  import XColony.Helpers

  match_codes ([
    size: 16,
    codes: %{
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
    }
  ])

  defstruct [:size, :key, :version, :correlation_id, :response_code]



  def handler(code, size, _) do
    %XColony.Response{
      size: size,
      response_code: code
    }
  end

end
