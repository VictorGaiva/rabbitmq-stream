defmodule RabbitMQStream.Message.Helpers do
  @type command ::
          :declare_publisher
          | :publish
          | :publish_confirm
          | :publish_error
          | :query_publisher_sequence
          | :delete_publisher
          | :subscribe
          | :deliver
          | :credit
          | :store_offset
          | :query_offset
          | :unsubscribe
          | :create_stream
          | :delete_stream
          | :query_metadata
          | :metadata_update
          | :peer_properties
          | :sasl_handshake
          | :sasl_authenticate
          | :tune
          | :open
          | :close
          | :heartbeat
          | :route
          | :partitions
          | :consumer_update
          | :exchange_command_versions
          | :stream_stats
          | :create_super_stream
          | :delete_super_stream

  @type response ::
          :ok
          | :stream_does_not_exist
          | :subscription_id_already_exists
          | :subscription_id_does_not_exist
          | :stream_already_exists
          | :stream_not_available
          | :sasl_mechanism_not_supported
          | :authentication_failure
          | :sasl_error
          | :sasl_challenge
          | :sasl_authentication_failure_loopback
          | :virtual_host_access_failure
          | :unknown_frame
          | :frame_too_large
          | :internal_error
          | :access_refused
          | :precondition_failed
          | :publisher_does_not_exist
          | :no_offset

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

  def encode_command(command) do
    @codes[command]
  end

  def decode_command(key) do
    @commands[Bitwise.band(key, 0b0111_1111_1111_1111)]
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

  def decode_code(code) do
    @response_codes[code]
  end

  def encode_code(atom) do
    @code_responses[atom]
  end

  def encode_string(value) when is_atom(value) do
    encode_string(Atom.to_string(value))
  end

  def encode_string(nil) do
    <<-1::integer-size(16)>>
  end

  def encode_string(str) do
    <<byte_size(str)::integer-size(16), str::binary>>
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

  def encode_map(nil) do
    encode_array([])
  end

  def encode_map(list) do
    list
    |> Enum.map(fn {key, value} -> encode_string(key) <> encode_string(value) end)
    |> encode_array()
  end

  def decode_string(<<size::integer-size(16), text::binary-size(size), rest::binary>>) do
    {rest, to_string(text)}
  end

  def decode_array("", _) do
    {"", []}
  end

  def decode_array(<<0::integer-size(32), buffer::binary>>, _) do
    {buffer, []}
  end

  def decode_array(<<size::integer-size(32), buffer::binary>>, foo) do
    Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
      foo.(buffer, acc)
    end)
  end
end
