defmodule RabbitMQStream.Message do
  @moduledoc """
  Module for creating and encoding new Messages, and decoding frames.
  """

  require Logger

  alias RabbitMQStream.Message.{Request, Response, Encoder}

  import RabbitMQStream.Helpers

  defmodule Code do
    @moduledoc false

    match_codes(%{
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
    })
  end

  defmodule Command do
    @moduledoc false

    match_codes(%{
      # Client, Yes
      0x0001 => :declare_publisher,
      # Client, No
      0x0002 => :publish,
      # Server, No
      0x0003 => :publish_confirm,
      # Server, No
      0x0004 => :publish_error,
      # Client, Yes
      0x0005 => :query_publisher_sequence,
      # Client, Yes
      0x0006 => :delete_publisher,
      # Client, Yes
      0x0007 => :subscribe,
      # Server, No
      0x0008 => :deliver,
      # Client, No
      0x0009 => :credit,
      # Client, No
      0x000A => :store_offset,
      # Client, Yes
      0x000B => :query_offset,
      # Client, Yes
      0x000C => :unsubscribe,
      # Client, Yes
      0x000D => :create_stream,
      # Client, Yes
      0x000E => :delete_stream,
      # Client, Yes
      0x000F => :query_metadata,
      # Server, No
      0x0010 => :metadata_update,
      # Client, Yes
      0x0011 => :peer_properties,
      # Client, Yes
      0x0012 => :sasl_handshake,
      # Client, Yes
      0x0013 => :sasl_authenticate,
      # Server, Yes
      0x0014 => :tune,
      # Client, Yes
      0x0015 => :open,
      # Client & Server, Yes
      0x0016 => :close,
      # Client & Server, No
      0x0017 => :heartbeat
    })
  end

  def encode_request!(conn, command, opts) do
    conn
    |> Request.new!(command, opts)
  end

  def encode_response!(conn, command, opts) do
    conn
    |> Response.new!(command, opts)
  end
end
