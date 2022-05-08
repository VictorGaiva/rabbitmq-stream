defmodule XColonyTest.MacrosHelper do
  import XColony.Helpers

  match_codes ([
    size: 16,
    codes: %{
      0x01 => :ok,
      0x02 => :stream_does_not_exist,
    }
  ])
end
