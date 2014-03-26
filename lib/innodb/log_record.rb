# -*- encoding : utf-8 -*-

# An InnoDB transaction log block.
class Innodb::LogRecord
  # Start and end LSNs for this record.
  attr_accessor :lsn

  # The size (in bytes) of the record.
  attr_reader :size

  attr_reader :preamble

  attr_reader :payload

  # InnoDB log record types.
  RECORD_TYPES =
    {
       1 => :MLOG_1BYTE,                 2 => :MLOG_2BYTE,
       4 => :MLOG_4BYTE,                 8 => :MLOG_8BYTE,
       9 => :REC_INSERT,                10 => :REC_CLUST_DELETE_MARK,
      11 => :REC_SEC_DELETE_MARK,       13 => :REC_UPDATE_IN_PLACE,
      14 => :REC_DELETE,                15 => :LIST_END_DELETE,
      16 => :LIST_START_DELETE,         17 => :LIST_END_COPY_CREATED,
      18 => :PAGE_REORGANIZE,           19 => :PAGE_CREATE,
      20 => :UNDO_INSERT,               21 => :UNDO_ERASE_END,
      22 => :UNDO_INIT,                 23 => :UNDO_HDR_DISCARD,
      24 => :UNDO_HDR_REUSE,            25 => :UNDO_HDR_CREATE,
      26 => :REC_MIN_MARK,              27 => :IBUF_BITMAP_INIT,
      28 => :LSN,                       29 => :INIT_FILE_PAGE,
      30 => :WRITE_STRING,              31 => :MULTI_REC_END,
      32 => :DUMMY_RECORD,              33 => :FILE_CREATE,
      34 => :FILE_RENAME,               35 => :FILE_DELETE,
      36 => :COMP_REC_MIN_MARK,         37 => :COMP_PAGE_CREATE,
      38 => :COMP_REC_INSERT,           39 => :COMP_REC_CLUST_DELETE_MARK,
      40 => :COMP_REC_SEC_DELETE_MARK,  41 => :COMP_REC_UPDATE_IN_PLACE,
      42 => :COMP_REC_DELETE,           43 => :COMP_LIST_END_DELETE,
      44 => :COMP_LIST_START_DELETE,    45 => :COMP_LIST_END_COPY_CREATE,
      46 => :COMP_PAGE_REORGANIZE,      47 => :FILE_CREATE2,
      48 => :ZIP_WRITE_NODE_PTR,        49 => :ZIP_WRITE_BLOB_PTR,
      50 => :ZIP_WRITE_HEADER,          51 => :ZIP_PAGE_COMPRESS,
    }

  # Types of undo log segments.
  UNDO_TYPES = { 1 => :UNDO_INSERT, 2 => :UNDO_UPDATE }

  def read(cursor)
    origin = cursor.position
    @preamble = read_preamble(cursor)
    @payload = read_payload(@preamble[:type], cursor)
    @size = cursor.position - origin
  end

  # Dump the contents of the record.
  def dump
    pp({:lsn => lsn, :size => size, :content => @preamble.merge(@payload)})
  end

  # Single record flag is masked in the record type.
  SINGLE_RECORD_MASK = 0x80
  RECORD_TYPE_MASK   = 0x7f

  # Return a preamble of the first record in this block.
  def read_preamble(c)
    type_and_flag = c.name("type") { c.get_uint8 }
    type = type_and_flag & RECORD_TYPE_MASK
    type = RECORD_TYPES[type] || type
    # Whether this is a single record for a single page.
    single_record = (type_and_flag & SINGLE_RECORD_MASK) > 0
    case type
    when :MULTI_REC_END, :DUMMY_RECORD
      { :type => type }
    else
      {
        :type           => type,
        :single_record  => single_record,
        :space          => c.name("space") { c.get_ic_uint32 },
        :page_number    => c.name("page_number") { c.get_ic_uint32 },
      }
    end
  end

  # Read the index part of a log record for a compact record insert.
  # Ref. mlog_parse_index
  def read_index(c)
    n_cols = c.name("n_cols") { c.get_uint16 }
    n_uniq = c.name("n_uniq") { c.get_uint16 }
    cols = n_cols.times.collect do
      info = c.name("field_info") { c.get_uint16 }
      {
        :mtype  => ((info + 1) & 0x7fff) <= 1 ? :BINARY : :FIXBINARY,
        :prtype => (info & 0x8000) != 0 ? :NOT_NULL : nil,
        :length => info & 0x7fff
      }
    end
    {
      :n_cols => n_cols,
      :n_uniq => n_uniq,
      :cols   => cols,
    }
  end

  # Flag of whether an insert log record contains info and status.
  INFO_AND_STATUS_MASK = 0x1

  # Read the insert record into page part of a insert log.
  # Ref. page_cur_parse_insert_rec
  def read_insert_record(c)
    page_offset = c.name("page_offset") { c.get_uint16 }
    end_seg_len = c.name("end_seg_len") { c.get_ic_uint32 }

    if (end_seg_len & INFO_AND_STATUS_MASK) != 0
      info_and_status_bits = c.get_uint8
      origin_offset = c.get_ic_uint32
      mismatch_index = c.get_ic_uint32
    end

    {
      :page_offset => page_offset,
      :end_seg_len => end_seg_len >> 1,
      :info_and_status_bits => info_and_status_bits,
      :origin_offset => origin_offset,
      :mismatch_index => mismatch_index,
      :record => c.name("record") { c.get_bytes(end_seg_len >> 1) },
    }
  end

  # Read the log record for an in-place update.
  # Ref. btr_cur_parse_update_in_place
  def read_update_in_place_record(c)
    {
      :flags => c.name("flags") { c.get_uint8 },
      :sys_fields => read_sys_fields(c),
      :rec_offset => c.name("rec_offset") { c.get_uint16 },
      :update_index => read_update_index(c),
    }
  end

  LENGTH_NULL = 0xFFFFFFFF

  # Read the update vector for an update log record.
  # Ref. row_upd_index_parse
  def read_update_index(c)
    info_bits = c.name("info_bits") { c.get_uint8 }
    n_fields  = c.name("n_fields") { c.get_ic_uint32 }
    fields = n_fields.times.collect do
      {
        :field_no => c.name("field_no") { c.get_ic_uint32 },
        :len      => len = c.name("len") { c.get_ic_uint32 },
        :data     => c.name("data") { len != LENGTH_NULL ? c.get_bytes(len) : :NULL },
      }
    end
    {
      :info_bits => info_bits,
      :n_fields  => n_fields,
      :fields    => fields,
    }
  end

  # Read system fields values in a log record.
  # Ref. row_upd_parse_sys_vals
  def read_sys_fields(c)
    {
      :trx_id_pos => c.name("trx_id_pos") { c.get_ic_uint32 },
      :roll_ptr   => c.name("roll_ptr") { c.get_bytes(7) },
      :trx_id     => c.name("trx_id") { c.get_ic_uint64 },
    }
  end

  # Read the log record for delete marking or unmarking of a clustered
  # index record.
  # Ref. btr_cur_parse_del_mark_set_clust_rec
  def read_clust_delete_mark(c)
    {
      :flags => c.name("flags") { c.get_uint8 },
      :value => c.name("value") { c.get_uint8 },
      :sys_fields => c.name("sys_fields") { read_sys_fields(c) },
      :offset => c.name("offset") { c.get_uint16 },
    }
  end

  def read_payload(type, c)
    case type
    when :MLOG_1BYTE, :MLOG_2BYTE, :MLOG_4BYTE
      {
        :page_offset => c.name("page_offset") { c.get_uint16 },
        :value       => c.name("value")  { c.get_ic_uint32 }
      }
    when :MLOG_8BYTE
      {
        :offset   => c.name("offset") { c.get_uint16 },
        :value    => c.name("value")  { c.get_ic_uint64 }
      }
    when :UNDO_HDR_CREATE, :UNDO_HDR_REUSE
      {
        :trx_id   => c.name("trx_id") { c.get_ic_uint64 }
      }
    when :UNDO_INSERT
      {
        :length   => len = c.name("length") { c.get_uint16 },
        :value    => c.name("value") { c.get_bytes(len) }
      }
    when :REC_INSERT
      {
        :record   => c.name("record") { read_insert_record(c) }
      }
    when :COMP_REC_INSERT
      {
        :index    => c.name("index")  { read_index(c) },
        :record   => c.name("record") { read_insert_record(c) }
      }
    when :COMP_REC_UPDATE_IN_PLACE
      {
        :index    => c.name("index")  { read_index(c) },
        :record   => c.name("record") { read_update_in_place_record(c) }
      }
    when :REC_UPDATE_IN_PLACE
      {
        :record   => c.name("record") { read_update_in_place_record(c) }
      }
    when :WRITE_STRING
      {
        :offset   => c.name("offset") { c.get_uint16 },
        :length   => length = c.name("length") { c.get_uint16 },
        :value    => c.name("value")  { c.get_bytes(length) },
      }
    when :UNDO_INIT
      {
        :type     => c.name("type")   { UNDO_TYPES[c.get_ic_uint32] }
      }
    when :FILE_CREATE, :FILE_DELETE
      {
        :name_len => len = c.name("name_len") { c.get_uint16 },
        :name     => c.name("name") { c.get_bytes(len) },
      }
    when :FILE_CREATE2
      {
        :flags    => c.name("flags") { c.get_uint32 },
        :name_len => len = c.name("name_len") { c.get_uint16 },
        :name     => c.name("name") { c.get_bytes(len) },
      }
    when :FILE_RENAME
      {
        :old => {
          :name_len => len = c.name("name_len") { c.get_uint16 },
          :name     => c.name("name") { c.get_bytes(len) },
        },
        :new => {
          :name_len => len = c.name("name_len") { c.get_uint16 },
          :name     => c.name("name") { c.get_bytes(len) },
        }
      }
    when :COMP_REC_CLUST_DELETE_MARK
      {
        :index    => c.name("index")  { read_index(c) },
        :record   => c.name("record") { read_clust_delete_mark(c) }
      }
    when :REC_CLUST_DELETE_MARK
      {
        :record   => c.name("record") { read_clust_delete_mark(c) }
      }
    when :COMP_REC_SEC_DELETE_MARK
      {
        :index    => c.name("index")  { read_index(c) },
        :value    => c.name("value")  { c.get_uint8 },
        :offset   => c.name("offset") { c.get_uint16 },
      }
    when :REC_SEC_DELETE_MARK
      {
        :value    => c.name("value")  { c.get_uint8 },
        :offset   => c.name("offset") { c.get_uint16 },
      }
    when :REC_DELETE
      {
        :offset   => c.name("offset") { c.get_uint16 },
      }
    when :COMP_REC_DELETE
      {
        :index    => c.name("index")  { read_index(c) },
        :offset   => c.name("offset") { c.get_uint16 },
      }
    when :REC_MIN_MARK, :COMP_REC_MIN_MARK
      {
        :offset   => c.name("offset") { c.get_uint16 },
      }
    when :LIST_START_DELETE, :LIST_END_DELETE
      {
        :offset   => c.name("offset") { c.get_uint16 },
      }
    when :COMP_LIST_START_DELETE, :COMP_LIST_END_DELETE
      {
        :index    => c.name("index")  { read_index(c) },
        :offset   => c.name("offset") { c.get_uint16 },
      }
    when :LIST_END_COPY_CREATED
      {
        :length   => len = c.name("length") { c.get_uint32 },
        :data     => c.name("data") { c.get_bytes(len) }
      }
    when :COMP_LIST_END_COPY_CREATE
      {
        :index    => c.name("index")  { read_index(c) },
        :length   => len = c.name("length") { c.get_uint32 },
        :data     => c.name("data") { c.get_bytes(len) }
      }
    when :COMP_PAGE_REORGANIZE
      {
        :index    => c.name("index")  { read_index(c) },
      }
    when :DUMMY_RECORD, :MULTI_REC_END, :INIT_FILE_PAGE,
         :IBUF_BITMAP_INIT, :PAGE_CREATE, :COMP_PAGE_CREATE,
         :PAGE_REORGANIZE, :UNDO_ERASE_END, :UNDO_HDR_DISCARD
      {}
    else
      raise "Unsupported log record type: #{type.to_s}"
    end
  end
end
