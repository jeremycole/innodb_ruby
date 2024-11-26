# frozen_string_literal: true

# A set of classes for parsing and working with InnoDB data files.

module Innodb
  @debug = false

  def self.debug?
    @debug == true
  end

  def self.debug=(value)
    @debug = value
  end
end

require "digest/crc32c"
require "innodb/util/buffer_cursor"
require "innodb/util/read_bits_at_offset"
require "innodb/util/hex_format"

require "innodb/version"
require "innodb/stats"
require "innodb/checksum"
require "innodb/mysql_collation"
require "innodb/mysql_collations"
require "innodb/mysql_type"
require "innodb/record_describer"
require "innodb/sys_data_dictionary"
require "innodb/sdi"
require "innodb/sdi/sdi_object"
require "innodb/sdi/table"
require "innodb/sdi/table_column"
require "innodb/sdi/table_index"
require "innodb/sdi/table_index_element"
require "innodb/sdi/tablespace"
require "innodb/sdi_data_dictionary"
require "innodb/data_dictionary"
require "innodb/page"
require "innodb/page/blob"
require "innodb/page/fsp_hdr_xdes"
require "innodb/page/ibuf_bitmap"
require "innodb/page/inode"
require "innodb/page/index"
require "innodb/page/trx_sys"
require "innodb/page/sdi"
require "innodb/page/sdi_blob"
require "innodb/page/sys"
require "innodb/page/undo_log"
require "innodb/data_type"
require "innodb/data_type/bit"
require "innodb/data_type/blob"
require "innodb/data_type/character"
require "innodb/data_type/date"
require "innodb/data_type/datetime"
require "innodb/data_type/decimal"
require "innodb/data_type/enum"
require "innodb/data_type/floating_point"
require "innodb/data_type/innodb_roll_pointer"
require "innodb/data_type/innodb_transaction_id"
require "innodb/data_type/integer"
require "innodb/data_type/set"
require "innodb/data_type/time"
require "innodb/data_type/timestamp"
require "innodb/data_type/year"
require "innodb/record"
require "innodb/field"
require "innodb/space"
require "innodb/system"
require "innodb/history"
require "innodb/history_list"
require "innodb/ibuf_bitmap"
require "innodb/ibuf_index"
require "innodb/inode"
require "innodb/index"
require "innodb/log_record"
require "innodb/log_block"
require "innodb/log"
require "innodb/lsn"
require "innodb/log_group"
require "innodb/log_reader"
require "innodb/undo_log"
require "innodb/undo_record"
require "innodb/xdes"
