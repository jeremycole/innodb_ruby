require "innodb/cursor"
require "pp"

class Innodb::LogBlock
  BLOCK_SIZE = 512

  HEADER_SIZE = 12
  HEADER_START = 0

  TRAILER_SIZE = 4
  TRAILER_START = BLOCK_SIZE - TRAILER_SIZE

  RECORD_START = HEADER_START + HEADER_SIZE

# Header:
#define	LOG_BLOCK_HDR_NO	0	/* block number which must be > 0 and
#define	LOG_BLOCK_HDR_DATA_LEN	4	/* number of bytes of log written to
#define	LOG_BLOCK_FIRST_REC_GROUP 6	/* offset of the first start of an
#define LOG_BLOCK_CHECKPOINT_NO	8	/* 4 lower bytes of the value of

# Trailer:
#define	LOG_BLOCK_CHECKSUM	0	/* 4 byte checksum of the log block

#/* Offsets for a checkpoint field */
#define LOG_CHECKPOINT_NO		0
#define LOG_CHECKPOINT_LSN		8
#define LOG_CHECKPOINT_OFFSET		16
#define LOG_CHECKPOINT_LOG_BUF_SIZE	20
#define	LOG_CHECKPOINT_ARCHIVED_LSN	24
#define	LOG_CHECKPOINT_GROUP_ARRAY	32

  def initialize(buffer)
    @buffer = buffer
  end

  def cursor(offset)
    Innodb::Cursor.new(self, offset)
  end

  def data(offset, length)
    @buffer[offset...(offset + length)]
  end

  def header
    @header ||= begin
      c = cursor(HEADER_START)
      {
        :block            => c.get_uint32,
        :data_length      => c.get_uint16,
        :first_rec_group  => c.get_uint16,
        :checkpoint_no    => c.get_uint32,
      }
    end
  end

  def trailer
    @trailer ||= begin
      c = cursor(TRAILER_START)
      {
        :checksum => c.get_uint32,
      }
    end
  end

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
  RECORD_TYPE_MASK = 0x7f
  def record
    @record ||= begin
      if header[:first_rec_group] != 0
        c = cursor(header[:first_rec_group])
        type_and_flag = c.get_uint8
        type = type_and_flag & RECORD_TYPE_MASK
        type = RECORD_TYPES[type] || type
        single_record = (type_and_flag & SINGLE_RECORD_MASK) == SINGLE_RECORD_MASK
        {
          :type           => type,
          :single_record  => single_record,
          :content        => record_content(type, c.position),
          :space          => c.get_ic_uint32,
          :page_number    => c.get_ic_uint32,
        }
      end
    end
  end

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