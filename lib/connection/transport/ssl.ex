defmodule RabbitMQStream.Connection.Transport.SSL do
  @behaviour RabbitMQStream.Connection.Transport

  def connect(options) do
    with {:ok, socket} <-
           :ssl.connect(
             String.to_charlist(options[:host]),
             options[:port],
             Keyword.merge(options[:ssl_opts], binary: true, active: true)
           ),
         :ok <- :ssl.controlling_process(socket, self()) do
      {:ok, socket}
    end
  end

  def close(socket) do
    :ssl.close(socket)
  end

  def send(socket, data) do
    :ssl.send(socket, data)
  end
end
