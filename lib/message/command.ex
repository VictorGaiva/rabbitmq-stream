defmodule RabbitStream.Message.Command do
  import RabbitStream.Helpers

  defmodule Code do
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
      0x000D => :create,
      # Client, Yes
      0x000E => :delete,
      # Client, Yes
      0x000F => :metadata,
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
end
