defmodule XColony.Response.Command do
  import XColony.Helpers
  match_codes( [
    size: 16,
    codes: %{
      0x0001 => :declare_publisher, #Client, Yes
      0x0002 => :publish, #Client, No
      0x0003 => :publish_confirm, #Server, No
      0x0004 => :publish_error, #Server, No
      0x0005 => :query_publisher_sequence, #Client, Yes
      0x0006 => :delete_publisher, #Client, Yes
      0x0007 => :subscribe, #Client, Yes
      0x0008 => :deliver, #Server, No
      0x0009 => :credit, #Client, No
      0x000a => :store_offset, #Client, No
      0x000b => :query_offset, #Client, Yes
      0x000c => :unsubscribe, #Client, Yes
      0x000d => :create, #Client, Yes
      0x000e => :delete, #Client, Yes
      0x000f => :metadata, #Client, Yes
      0x0010 => :metadata_update, #Server, No
      0x0011 => :peer_properties, #Client, Yes
      0x0012 => :sasl_handshake, #Client, Yes
      0x0013 => :sasl_authenticate, #Client, Yes
      0x0014 => :tune, #Server, Yes
      0x0015 => :open, #Client, Yes
      0x0016 => :close, #Client & Server, Yes
      0x0017 => :heartbeat, #Client & Server, No
      0x0018 => :route, #Client, Yes
      0x0019 => :partitions, #Client, Yes
    }
  ])



  def handler(s, size, data) do
    IO.inspect(s)
    IO.inspect(size)
    IO.inspect(data)
  end

end
