# -*- encoding : utf-8 -*-

# A set of classes for parsing and working with InnoDB data files.

module Innodb
  @@debug = false

  def self.debug?
    @@debug == true
  end

  def self.debug=(value)
    @@debug = value
  end
end

require "pp"
require "enumerator"
require "innodb/util/buffer_cursor"
require "innodb/util/read_bits_at_offset"

require "innodb/version"
require "innodb/stats"
require "innodb/checksum"
require "innodb/record_describer"
require "innodb/data_dictionary"
require "innodb/page"
require "innodb/page/blob"
require "innodb/page/fsp_hdr_xdes"
require "innodb/page/ibuf_bitmap"
require "innodb/page/inode"
require "innodb/page/index"
require "innodb/page/index_uncompressed"
require "innodb/page/index_compressed"
require "innodb/page/trx_sys"
require "innodb/page/sys"
require "innodb/page/undo_log"
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
