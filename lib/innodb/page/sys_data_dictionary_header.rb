# frozen_string_literal: true

module Innodb
  class Page
    class SysDataDictionaryHeader < Page
      Header = Struct.new(
        :max_row_id,
        :max_table_id,
        :max_index_id,
        :max_space_id,
        :unused_mix_id_low,
        :indexes,
        :unused_space,
        :fseg,
        keyword_init: true
      )

      # The position of the data dictionary header within the page.
      def pos_data_dictionary_header
        pos_page_body
      end

      # The size of the data dictionary header.
      def size_data_dictionary_header
        ((8 * 3) + (4 * 7) + 4 + Innodb::FsegEntry::SIZE)
      end

      # Parse the data dictionary header from the page.
      def data_dictionary_header
        cursor(pos_data_dictionary_header).name("data_dictionary_header") do |c|
          Header.new(
            max_row_id: c.name("max_row_id") { c.read_uint64 },
            max_table_id: c.name("max_table_id") { c.read_uint64 },
            max_index_id: c.name("max_index_id") { c.read_uint64 },
            max_space_id: c.name("max_space_id") { c.read_uint32 },
            unused_mix_id_low: c.name("unused_mix_id_low") { c.read_uint32 },
            indexes: c.name("indexes") do
              {
                SYS_TABLES: c.name("SYS_TABLES") do
                  {
                    PRIMARY: c.name("PRIMARY") { c.read_uint32 },
                    ID: c.name("ID") { c.read_uint32 },
                  }
                end,
                SYS_COLUMNS: c.name("SYS_COLUMNS") do
                  {
                    PRIMARY: c.name("PRIMARY") { c.read_uint32 },
                  }
                end,
                SYS_INDEXES: c.name("SYS_INDEXES") do
                  {
                    PRIMARY: c.name("PRIMARY") { c.read_uint32 },
                  }
                end,
                SYS_FIELDS: c.name("SYS_FIELDS") do
                  {
                    PRIMARY: c.name("PRIMARY") { c.read_uint32 },
                  }
                end,
              }
            end,
            unused_space: c.name("unused_space") { c.read_bytes(4) },
            fseg: c.name("fseg") { Innodb::FsegEntry.get_inode(@space, c) }
          )
        end
      end

      def each_region(&block)
        return enum_for(:each_region) unless block_given?

        super

        yield Region.new(
          offset: pos_data_dictionary_header,
          length: size_data_dictionary_header,
          name: :data_dictionary_header,
          info: "Data Dictionary Header"
        )

        nil
      end

      def dump
        super

        puts
        puts "data_dictionary header:"
        pp data_dictionary_header
      end
    end
  end
end
