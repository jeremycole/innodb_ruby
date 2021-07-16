# frozen_string_literal: true

require 'forwardable'

# A specialized class for TRX_SYS pages, which contain various information
# about the transaction system within InnoDB. Only one TRX_SYS page exists in
# any given InnoDB installation, and it is page 5 of the system tablespace
# (space 0), most commonly named "ibdata1".
#
# The basic structure of a TRX_SYS page is: FIL header, TRX_SYS header,
# empty space, master binary log information, empty space, local binary
# log information, empty space, doublewrite information (repeated twice),
# empty space, and FIL trailer.
module Innodb
  class Page
    class TrxSys < Page
      extend Forwardable

      specialization_for :TRX_SYS

      RsegSlot = Struct.new(
        :offset,
        :space_id,
        :page_number,
        keyword_init: true
      )

      MysqlLogInfo = Struct.new(
        :magic_n,
        :offset,
        :name,
        keyword_init: true
      )

      DoublewritePageInfo = Struct.new(
        :magic_n,
        :page_number,
        keyword_init: true
      )

      DoublewriteInfo = Struct.new(
        :fseg,
        :page_info,
        :space_id_stored,
        keyword_init: true
      )

      Header = Struct.new(
        :trx_id,
        :fseg,
        :rsegs,
        :binary_log,
        :master_log,
        :doublewrite,
        keyword_init: true
      )

      # The TRX_SYS header immediately follows the FIL header.
      def pos_trx_sys_header
        pos_page_body
      end

      def size_trx_sys_header
        8 + Innodb::FsegEntry::SIZE
      end

      def pos_rsegs_array
        pos_trx_sys_header + size_trx_sys_header
      end

      def size_mysql_log_info
        4 + 8 + 100
      end

      # The master's binary log information is located 2000 bytes from the end of
      # the page.
      def pos_mysql_master_log_info
        size - 2_000
      end

      # The local binary log information is located 1000 bytes from the end of
      # the page.
      def pos_mysql_binary_log_info
        size - 1_000
      end

      # The doublewrite buffer information is located 200 bytes from the end of
      # the page.
      def pos_doublewrite_info
        size - 200
      end

      def size_doublewrite_info
        Innodb::FsegEntry::SIZE + (2 * (4 + 4 + 4)) + 4
      end

      # A magic number present in each MySQL binary log information structure,
      # which helps identify whether the structure is populated or not.
      MYSQL_LOG_MAGIC_N = 873_422_344

      # A magic number present in each doublewrite buffer information structure,
      # which helps identify whether the structure is populated or not.
      DOUBLEWRITE_MAGIC_N = 536_853_855

      # A magic number present in the overall doublewrite buffer structure,
      # which identifies whether the space id is stored.
      DOUBLEWRITE_SPACE_ID_STORED_MAGIC_N = 1_783_657_386

      N_RSEGS = 128

      def rsegs_array(cursor)
        @rsegs_array ||= N_RSEGS.times.each_with_object([]) do |n, a|
          cursor.name("slot[#{n}]") do |c|
            slot = RsegSlot.new(
              offset: c.position,
              space_id: c.name('space_id') { Innodb::Page.maybe_undefined(c.read_uint32) },
              page_number: c.name('page_number') { Innodb::Page.maybe_undefined(c.read_uint32) }
            )
            a << slot if slot.space_id && slot.page_number
          end
        end
      end

      # Read a MySQL binary log information structure from a given position.
      def mysql_log_info(cursor, offset)
        cursor.peek(offset) do |c|
          magic_n = c.name('magic_n') { c.read_uint32 } == MYSQL_LOG_MAGIC_N
          break unless magic_n

          MysqlLogInfo.new(
            magic_n: magic_n,
            offset: c.name('offset') { c.read_uint64 },
            name: c.name('name') { c.read_bytes(100) }
          )
        end
      end

      # Read a single doublewrite buffer information structure from a given cursor.
      def doublewrite_page_info(cursor)
        magic_n = cursor.name('magic_n') { cursor.read_uint32 }

        DoublewritePageInfo.new(
          magic_n: magic_n,
          page_number: [0, 1].map { |n| cursor.name("page[#{n}]") { cursor.read_uint32 } }
        )
      end

      # Read the overall doublewrite buffer structures
      def doublewrite_info(cursor)
        cursor.peek(pos_doublewrite_info) do |c_doublewrite|
          c_doublewrite.name('doublewrite') do |c|
            DoublewriteInfo.new(
              fseg: c.name('fseg') { Innodb::FsegEntry.get_inode(@space, c) },
              page_info: [0, 1].map { |n| c.name("group[#{n}]") { doublewrite_page_info(c) } },
              space_id_stored: (c.name('space_id_stored') { c.read_uint32 } == DOUBLEWRITE_SPACE_ID_STORED_MAGIC_N)
            )
          end
        end
      end

      # Read the TRX_SYS headers and other information.
      def trx_sys
        @trx_sys ||= cursor(pos_trx_sys_header).name('trx_sys') do |c|
          Header.new(
            trx_id: c.name('trx_id') { c.read_uint64 },
            fseg: c.name('fseg') { Innodb::FsegEntry.get_inode(@space, c) },
            rsegs: c.name('rsegs') { rsegs_array(c) },
            binary_log: c.name('binary_log') { mysql_log_info(c, pos_mysql_binary_log_info) },
            master_log: c.name('master_log') { mysql_log_info(c, pos_mysql_master_log_info) },
            doublewrite: doublewrite_info(c)
          )
        end
      end

      def_delegator :trx_sys, :trx_id
      def_delegator :trx_sys, :fseg
      def_delegator :trx_sys, :rsegs
      def_delegator :trx_sys, :binary_log
      def_delegator :trx_sys, :master_log
      def_delegator :trx_sys, :doublewrite

      def each_region(&block)
        return enum_for(:each_region) unless block_given?

        super(&block)

        yield Region.new(
          offset: pos_trx_sys_header,
          length: size_trx_sys_header,
          name: :trx_sys_header,
          info: 'Transaction System Header'
        )

        rsegs.each do |rseg|
          yield Region.new(
            offset: rseg[:offset],
            length: 4 + 4,
            name: :rseg,
            info: 'Rollback Segment'
          )
        end

        yield Region.new(
          offset: pos_mysql_binary_log_info,
          length: size_mysql_log_info,
          name: :mysql_binary_log_info,
          info: 'Binary Log Info'
        )

        yield Region.new(
          offset: pos_mysql_master_log_info,
          length: size_mysql_log_info,
          name: :mysql_master_log_info,
          info: 'Master Log Info'
        )

        yield Region.new(
          offset: pos_doublewrite_info,
          length: size_doublewrite_info,
          name: :doublewrite_info,
          info: 'Double Write Buffer Info'
        )

        nil
      end

      # Dump the contents of a page for debugging purposes.
      def dump
        super

        puts 'trx_sys:'
        pp trx_sys
        puts
      end
    end
  end
end
