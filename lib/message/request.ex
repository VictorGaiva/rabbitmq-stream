defmodule RabbitMQStream.Message.Request do
  @moduledoc false
  require Logger

  alias RabbitMQStream.Connection

  def new!(%Connection{} = conn, :peer_properties, _) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :peer_properties,
        %{
          "product" => "RabbitMQ Stream Client",
          "information" => "Elixir Client",
          "version" => "0.1.0",
          "platform" => "Elixir"
        }
      }
    })
  end

  def new!(%Connection{} = conn, :sasl_handshake, _) do
    :rabbit_stream_core.frame({:request, conn.correlation_sequence, :sasl_handshake})
  end

  def new!(%Connection{} = conn, :sasl_authenticate, _) do
    cond do
      Enum.member?(conn.mechanisms, "PLAIN") ->
        :rabbit_stream_core.frame({
          :request,
          conn.correlation_sequence,
          {
            :sasl_authenticate,
            "PLAIN",
            "\u0000#{conn.options[:username]}\u0000#{conn.options[:password]}"
          }
        })

      true ->
        raise "Unsupported SASL mechanism: #{conn.mechanisms}"
    end
  end

  def new!(%Connection{options: options}, :tune, _) do
    :rabbit_stream_core.frame({:tune, options[:frame_max], options[:heartbeat]})
  end

  def new!(%Connection{options: options, correlation_sequence: correlation_sequence}, :open, _) do
    :rabbit_stream_core.frame({:request, correlation_sequence, {:open, options[:vhost]}})
  end

  def new!(%Connection{}, :heartbeat, _) do
    :rabbit_stream_core.frame(:heartbeat)
  end

  def new!(%Connection{} = conn, :close, opts) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :close,
        opts[:code],
        opts[:reason]
      }
    })
  end

  def new!(%Connection{} = conn, :create_stream, opts) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :create_stream,
        opts[:name],
        Map.new(Keyword.drop(opts, [:name]))
      }
    })
  end

  def new!(%Connection{correlation_sequence: correlation_sequence}, :delete_stream, opts) do
    :rabbit_stream_core.frame({:request, correlation_sequence, {:delete_stream, opts[:name]}})
  end

  def new!(%Connection{}, :store_offset, opts) do
    :rabbit_stream_core.frame({
      :store_offset,
      opts[:offset_reference],
      opts[:stream_name],
      opts[:offset]
    })
  end

  def new!(%Connection{} = conn, :query_offset, opts) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :query_offset,
        opts[:offset_reference],
        opts[:stream_name]
      }
    })
  end

  def new!(%Connection{} = conn, :declare_publisher, opts) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :declare_publisher,
        conn.publisher_sequence,
        opts[:publisher_reference],
        opts[:stream_name]
      }
    })
  end

  def new!(%Connection{} = conn, :delete_publisher, opts) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :delete_publisher,
        opts[:publisher_id]
      }
    })
  end

  def new!(%Connection{} = conn, :query_metadata, opts) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :metadata,
        opts[:streams]
      }
    })
  end

  def new!(%Connection{} = conn, :query_publisher_sequence, opts) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :query_publisher_sequence,
        opts[:publisher_reference],
        opts[:stream_name]
      }
    })
  end

  def new!(%Connection{}, :publish, opts) do
    messages =
      for {publishing_id, message} <- opts[:published_messages], into: <<>> do
        <<publishing_id::unsigned-integer-size(64), byte_size(message)::integer-size(32), message::binary>>
      end

    :rabbit_stream_core.frame({
      :publish,
      opts[:publisher_id],
      Enum.count(opts[:published_messages]),
      messages
    })
  end

  def new!(%Connection{} = conn, :subscribe, opts) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :subscribe,
        opts[:subscription_id],
        opts[:stream_name],
        opts[:offset],
        opts[:credit],
        opts[:properties]
      }
    })
  end

  def new!(%Connection{} = conn, :unsubscribe, opts) do
    :rabbit_stream_core.frame({
      :request,
      conn.correlation_sequence,
      {
        :unsubscribe,
        opts[:subscription_id]
      }
    })
  end
end
