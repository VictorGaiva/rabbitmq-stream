defmodule RabbitStream.Frame do

  defstruct [
    :version,
    :correlation_id,
    :command
  ]

end
