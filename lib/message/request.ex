defmodule XColony.Message.Request do
  alias XColony.Message.Command.Code.{
    PeerProperties,
    SaslHandshake,
    SaslAuthenticate
  }

  alias __MODULE__, as: Request

  defstruct [
    :version,
    :correlation_id,
    :command,
    :data,
  ]

  def encode_string(nil)  do
    << -1::integer-size(16) >>
  end

  def encode_string(str)  do
    <<byte_size(str)::integer-size(16), str::binary>>
  end

  def encode_bytes(nil)  do
    << -1::integer-size(32) >>
  end

  def encode_bytes(str)  do
    <<byte_size(str)::integer-size(32), str::binary>>
  end

  def encode_array(arr) do
    arr = arr |> Enum.reverse() |> Enum.reduce(&<>/2)
    <<byte_size(arr)::integer-size(32), arr::binary>>
  end


  def encode!(%Request{command: %PeerProperties{}} = request) do
    properties = request.data[:properties]
      |> Enum.map(fn [key, value] -> encode_string(key) <> encode_string(value) end)
      |> encode_array()

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      properties::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %SaslHandshake{}} = request) do
    mechanism = encode_string(request.data[:mechanism])

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      mechanism::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end

  def encode!(%Request{command: %SaslAuthenticate{}} = request) do
    mechanism = encode_string(request.data[:mechanism])
    credentials = encode_bytes(request.data[:credentials])

    data = <<
      request.command.code::unsigned-integer-size(16),
      request.version::unsigned-integer-size(16),
      request.correlation_id::unsigned-integer-size(32),
      mechanism::binary,
      credentials::binary
    >>

    <<byte_size(data)::unsigned-integer-size(32), data::binary>>
  end



  def encode!(_, _ \\ [])

  def encode!(:declare_publisher, _opts) do
    0x0001
  end

  def encode!(:publish, _opts) do
    0x0002
  end
  def encode!(:query_publisher_sequence, _opts) do
    0x0005
  end
  def encode!(:delete_publisher, _opts) do
    0x0006
  end
  def encode!(:subscribe, _opts) do
    0x0007
  end
  def encode!(:credit, _opts) do
    0x0009
  end
  def encode!(:store_offset, _opts) do
    0x000a
  end
  def encode!(:query_offset, _opts) do
    0x000b
  end
  def encode!(:unsubscribe, _opts) do
    0x000c
  end
  def encode!(:create, _opts) do
    0x000d
  end
  def encode!(:delete, _opts) do
    0x000e
  end
  def encode!(:metadata, _opts) do
    0x000f
  end
  def encode!(:sasl_authenticate, _opts) do
    0x0013
  end
  def encode!(:open, _opts) do
    0x0015
  end
  def encode!(:close, _opts) do
    0x0016
  end
  def encode!(:heartbeat, _opts) do
    0x0017
  end
  def encode!(:route, _opts) do
    0x0018
  end
  def encode!(:partitions, _opts) do
    0x0019
  end

  def new!(:peer_properties, version, correlation_id ) do
    %Request{
      version: version,
      correlation_id: correlation_id,
      command: %PeerProperties{},
      data: [
        properties: [
          ["product","RabbitMQ Stream"],
          ["copyright","Copyright (c) 2021 VMware, Inc. or its affiliates."],
          ["information","Licensed under the MPL 2.0. See https://www.rabbitmq.com/"],
          ["version","0.0.1"],
          ["platform","Elixir"],
        ]
      ]
    }
  end

  def new!(:sasl_handshake, version, correlation_id) do
    %Request{
      version: version,
      correlation_id: correlation_id,
      command: %SaslHandshake{},
      data: [mechanism: "PLAIN"]
    }
  end

  def new!(:sasl_authenticate, version, correlation_id) do
    %Request{
      version: version,
      correlation_id: correlation_id,
      command: %SaslAuthenticate{},
      data: [
        mechanism: "PLAIN",
        credentials: "guest:guest"
      ]
    }
  end
end
