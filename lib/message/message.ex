defmodule RabbitMQStream.Message do
  @moduledoc """
  Module for creating and encoding new Messages, and decoding frames.
  """

  require Logger

  alias RabbitMQStream.Message.{Request, Response}

  def encode_request!(conn, command, opts) do
    conn
    |> Request.new!(command, opts)
  end

  def encode_response!(conn, command, opts) do
    conn
    |> Response.new!(command, opts)
  end
end
