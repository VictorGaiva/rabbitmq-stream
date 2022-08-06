defmodule Helpers.OsirisChunk do
  @moduledoc false
  @type chunk_type :: :chunk_user | :chunk_track_delta | :chunk_track_snapshot
  @spec decode_entry!(type :: chunk_type(), data :: binary()) :: {binary() | [ChunkTrackSnapshot.t()], binary()}

  @type t :: %{
          # <<chunk_type::integer-size(8)>>
          chunk_type: chunk_type(),
          # <<num_entries::unsigned-integer-size(16)>>
          num_entries: non_neg_integer(),
          # <<num_records::unsigned-integer-size(32)>>
          num_records: non_neg_integer(),
          # <<timestamp::integer-size(64)>>
          timestamp: integer(),
          # <<epoch::unsigned-integer-size(64)>>
          epoch: non_neg_integer(),
          # <<chunk_first_offset::unsigned-integer-size(64)>>
          chunk_first_offset: non_neg_integer(),
          # <<chunk_crc::integer-size(32)>>
          chunk_crc: integer(),
          # <<data_length::unsigned-integer-size(32)>>
          data_length: non_neg_integer(),
          # <<trailer_length::unsigned-integer-size(32)>>
          trailer_length: non_neg_integer(),
          # <<reserved::unsigned-integer-size(32)>>
          data_entries: binary() | [ChunkTrackSnapshot.t()],
          trailer_entries: binary()
        }
  @enforce_keys [
    :chunk_type,
    :num_entries,
    :num_records,
    :timestamp,
    :epoch,
    :chunk_first_offset,
    :chunk_crc,
    :data_length,
    :trailer_length,
    :data_entries
  ]
  defstruct [
    :chunk_type,
    :num_entries,
    :num_records,
    :timestamp,
    :epoch,
    :chunk_first_offset,
    :chunk_crc,
    :data_length,
    :trailer_length,
    :data_entries
  ]

  defmodule ChunkTrackSnapshot do
    @moduledoc false
    @type t :: %{
            type: :sequence | :offset,
            id_size: non_neg_integer(),
            id: binary(),
            chunk_id: non_neg_integer() | nil,
            sequence: non_neg_integer() | nil,
            offset: non_neg_integer() | nil
          }
    @enforce_keys [:type, :id_size, :id]
    defstruct [:type, :id_size, :id, :chunk_id, :sequence, :offset]

    defp do_decode!(<<
           0::unsigned-integer-size(8),
           id_size::unsigned-integer-size(8),
           id::binary-size(id_size),
           chunk_id::unsigned-integer-64,
           sequence::unsigned-integer-size(64),
           rest::binary
         >>) do
      {%ChunkTrackSnapshot{type: :sequence, id_size: id_size, id: id, chunk_id: chunk_id, sequence: sequence}, rest}
    end

    defp do_decode!(<<
           1::unsigned-integer-size(8),
           id_size::unsigned-integer-size(8),
           id::binary-size(id_size),
           offset::integer-size(64),
           rest::binary
         >>) do
      {%ChunkTrackSnapshot{type: :offset, id_size: id_size, id: id, offset: offset}, rest}
    end

    def decode_entries!(<<>>) do
      []
    end

    def decode_entries!(data) do
      {entry, data} = do_decode!(data)

      decode_entries!(data) ++ [entry]
    end
  end

  defmodule SubBatchEntry do
    @moduledoc false
    @type t :: %{
            # <<compression::integer-size(3)>>
            compression: integer(),
            # <<reserved::integer-size(4)>>
            reserved: integer(),
            # <<num_records::unsigned-integer-size(16)>>
            num_records: non_neg_integer(),
            # <<uncompressed_length::unsigned-integer-size(32)>>
            uncompressed_length: non_neg_integer(),
            # <<length::unsigned-integer-size(32)>>
            length: non_neg_integer(),
            # <<length::binary-size(length)>>
            body: binary()
          }

    @enforce_keys [:compression, :reserved, :num_records, :uncompressed_length, :length, :body]
    defstruct [:compression, :reserved, :num_records, :uncompressed_length, :length, :body]
  end

  defp decode_entry!(:chunk_user, <<0::1, length::unsigned-integer-size(31), rest::binary>>) do
    <<data::binary-size(length), rest::binary>> = rest
    {data, rest}
  end

  defp decode_entry!(:chunk_track_snapshot, <<0::1, length::unsigned-integer-size(31), rest::binary>>) do
    <<data::binary-size(length), rest::binary>> = rest

    {ChunkTrackSnapshot.decode_entries!(data), rest}
  end

  defp decode_entry!(:chunk_user, <<1::1, compression::integer-size(3), reserved::integer-size(4), rest::binary>>) do
    <<
      num_records::unsigned-integer-size(16),
      uncompressed_length::unsigned-integer-size(32),
      length::unsigned-integer-size(32),
      body::binary-size(length),
      rest::binary
    >> = rest

    entry = %SubBatchEntry{
      compression: compression,
      reserved: reserved,
      num_records: num_records,
      uncompressed_length: uncompressed_length,
      length: length,
      body: body
    }

    {entry, rest}
  end

  def decode!(<<0x5::unsigned-integer-size(4), 0x1::unsigned-integer-size(4), rest::binary>>) do
    <<
      chunk_type::integer-size(8),
      num_entries::unsigned-integer-size(16),
      num_records::unsigned-integer-size(32),
      timestamp::integer-size(64),
      epoch::unsigned-integer-size(64),
      chunk_first_offset::unsigned-integer-size(64),
      chunk_crc::integer-size(32),
      data_length::unsigned-integer-size(32),
      trailer_length::unsigned-integer-size(32),
      _reserved::unsigned-integer-size(32),
      data::binary
    >> = rest

    chunk_type =
      case chunk_type do
        1 ->
          :chunk_user

        2 ->
          :chunk_track_delta

        3 ->
          :chunk_track_snapshot

        _ ->
          raise "Unknown chunk type: #{chunk_type}"
      end

    {data_entries, _rest} =
      Enum.reduce(0..data_length, {[], data}, fn
        0, curr ->
          curr

        _, {acc, data} ->
          {entry, rest} = decode_entry!(chunk_type, data)
          {acc ++ [entry], rest}
      end)

    %__MODULE__{
      chunk_type: chunk_type,
      num_entries: num_entries,
      num_records: num_records,
      timestamp: timestamp,
      epoch: epoch,
      chunk_first_offset: chunk_first_offset,
      chunk_crc: chunk_crc,
      data_length: data_length,
      trailer_length: trailer_length,
      data_entries: data_entries
    }
  end
end
