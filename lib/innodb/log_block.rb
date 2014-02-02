# -*- encoding : utf-8 -*-

require "innodb/cursor"
require "pp"

# An InnoDB transaction log block.
class Innodb::LogBlock
  # Log blocks are fixed-length at 512 bytes in InnoDB.
  BLOCK_SIZE = 512

  # Offset of the header within the log block.
  HEADER_OFFSET = 0

  # Offset of the trailer within ths log block.
  TRAILER_OFFSET = BLOCK_SIZE - 4

  # Initialize a log block by passing in a 512-byte buffer containing the raw
  # log block contents.
  def initialize(buffer)
    unless buffer.size == BLOCK_SIZE
      raise "Log block buffer provided was not #{BLOCK_SIZE} bytes" 
    end

    @buffer = buffer
  end

  # Return an Innodb::Cursor object positioned at a specific offset.
  def cursor(offset)
    Innodb::Cursor.new(@buffer, offset)
  end

  # Return the log block header.
  def header
    @header ||= cursor(HEADER_OFFSET).name("header") do |c|
      {
        :block_number     => c.name("block_number")    { c.get_uint32 & (2**31-1) },
        :data_length      => c.name("data_length")     { c.get_uint16 },
        :first_rec_group  => c.name("first_rec_group") { c.get_uint16 },
        :checkpoint_no    => c.name("checkpoint_no")   { c.get_uint32 },
      }
    end
  end

  # Return the log block trailer.
  def trailer
    @trailer ||= cursor(TRAILER_OFFSET).name("trailer") do |c|
      {
        :checksum => c.name("checksum") { c.get_uint32 },
      }
    end
  end

  # The constants used by InnoDB for identifying different log record types.
  RECORD_TYPES = {
    1  => :MLOG_1BYTE,
    2  => :MLOG_2BYTE,
    4  => :MLOG_4BYTE,
    8  => :MLOG_8BYTE,
    9  => :REC_INSERT,
    10 => :REC_CLUST_DELETE_MARK,
    11 => :REC_SEC_DELETE_MARK,
    13 => :REC_UPDATE_IN_PLACE,
    14 => :REC_DELETE,
    15 => :LIST_END_DELETE,
    16 => :LIST_START_DELETE,
    17 => :LIST_END_COPY_CREATED,
    18 => :PAGE_REORGANIZE,
    19 => :PAGE_CREATE,
    20 => :UNDO_INSERT,
    21 => :UNDO_ERASE_END,
    22 => :UNDO_INIT,
    23 => :UNDO_HDR_DISCARD,
    24 => :UNDO_HDR_REUSE,
    25 => :UNDO_HDR_CREATE,
    26 => :REC_MIN_MARK,
    27 => :IBUF_BITMAP_INIT,
    28 => :LSN,
    29 => :INIT_FILE_PAGE,
    30 => :WRITE_STRING,
    31 => :MULTI_REC_END,
    32 => :DUMMY_RECORD,
    33 => :FILE_CREATE,
    34 => :FILE_RENAME,
    35 => :FILE_DELETE,
    36 => :COMP_REC_MIN_MARK,
    37 => :COMP_PAGE_CREATE,
    38 => :COMP_REC_INSERT,
    39 => :COMP_REC_CLUST_DELETE_MARK,
    40 => :COMP_REC_SEC_DELETE_MARK,
    41 => :COMP_REC_UPDATE_IN_PLACE,
    42 => :COMP_REC_DELETE,
    43 => :COMP_LIST_END_DELETE,
    44 => :COMP_LIST_START_DELETE,
    45 => :COMP_LIST_END_COPY_CREATE,
    46 => :COMP_PAGE_REORGANIZE,
    47 => :FILE_CREATE2,
    48 => :ZIP_WRITE_NODE_PTR,
    49 => :ZIP_WRITE_BLOB_PTR,
    50 => :ZIP_WRITE_HEADER,
    51 => :ZIP_PAGE_COMPRESS,
  }

  # Return the log record contents. (This is mostly unimplemented.)
  def record_content(record_type, offset)
    c = cursor(offset)
    case record_type
    when :MLOG_1BYTE
      c.get_uint8
    when :MLOG_2BYTE
      c.get_uint16
    when :MLOG_4BYTE
      c.get_uint32
    when :MLOG_8BYTE
      c.get_uint64
    when :UNDO_INSERT
    when :COMP_REC_INSERT
    end
  end

  SINGLE_RECORD_MASK = 0x80
  RECORD_TYPE_MASK   = 0x7f

  # Return the log record. (This is mostly unimplemented.)
  def record
    @record ||= begin
      if header[:first_rec_group] != 0
        cursor(header[:first_rec_group]).name("header") do |c|
          type_and_flag = c.name("type") { c.get_uint8 }
          type = type_and_flag & RECORD_TYPE_MASK
          type = RECORD_TYPES[type] || type
          single_record = (type_and_flag & SINGLE_RECORD_MASK) > 0
          {
            :type           => type,
            :single_record  => single_record,
            :content        => record_content(type, c.position),
            :space          => c.name("space") { c.get_ic_uint32 },
            :page_number    => c.name("page_number") { c.get_ic_uint32 },
          }
        end
      end
    end
  end

  # Dump the contents of a log block for debugging purposes.
  def dump
    puts
    puts "header:"
    pp header

    puts
    puts "trailer:"
    pp trailer

    puts
    puts "record:"
    pp record
  end
end
