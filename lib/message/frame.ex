defmodule RabbitMQStream.Message.Frame do
  @moduledoc false

  @commands %{
    0x0001 => :declare_publisher,
    0x0002 => :publish,
    0x0003 => :publish_confirm,
    0x0004 => :publish_error,
    0x0005 => :query_publisher_sequence,
    0x0006 => :delete_publisher,
    0x0007 => :subscribe,
    0x0008 => :deliver,
    0x0009 => :credit,
    0x000A => :store_offset,
    0x000B => :query_offset,
    0x000C => :unsubscribe,
    0x000D => :create_stream,
    0x000E => :delete_stream,
    0x000F => :query_metadata,
    0x0010 => :metadata_update,
    0x0011 => :peer_properties,
    0x0012 => :sasl_handshake,
    0x0013 => :sasl_authenticate,
    0x0014 => :tune,
    0x0015 => :open,
    0x0016 => :close,
    0x0017 => :heartbeat,
    0x0018 => :route,
    0x0019 => :partitions,
    0x001A => :consumer_update,
    0x001B => :exchange_command_versions,
    0x001C => :stream_stats,
    0x001D => :create_super_stream,
    0x001E => :delete_super_stream
  }

  @codes Enum.into(@commands, %{}, fn {code, command} -> {command, code} end)

  def command_to_code(command) do
    @codes[command]
  end

  def code_to_command(code) do
    @commands[code]
  end

  @response_codes %{
    0x01 => :ok,
    0x02 => :stream_does_not_exist,
    0x03 => :subscription_id_already_exists,
    0x04 => :subscription_id_does_not_exist,
    0x05 => :stream_already_exists,
    0x06 => :stream_not_available,
    0x07 => :sasl_mechanism_not_supported,
    0x08 => :authentication_failure,
    0x09 => :sasl_error,
    0x0A => :sasl_challenge,
    0x0B => :sasl_authentication_failure_loopback,
    0x0C => :virtual_host_access_failure,
    0x0D => :unknown_frame,
    0x0E => :frame_too_large,
    0x0F => :internal_error,
    0x10 => :access_refused,
    0x11 => :precondition_failed,
    0x12 => :publisher_does_not_exist,
    0x13 => :no_offset
  }

  @code_responses Enum.into(@response_codes, %{}, fn {code, command} -> {command, code} end)

  def response_code_to_atom(code) do
    @response_codes[code]
  end

  def atom_to_response_code(atom) do
    @code_responses[atom]
  end
end
