# frozen_string_literal: true

module Innodb
  class Page
    class SysIbufHeader < Page
      Header = Struct.new(
        :fseg,
        keyword_init: true
      )

      def pos_ibuf_header
        pos_page_body
      end

      def size_ibuf_header
        Innodb::FsegEntry::SIZE
      end

      def ibuf_header
        cursor(pos_ibuf_header).name("ibuf_header") do |c|
          Header.new(
            fseg: c.name("fseg") { Innodb::FsegEntry.get_inode(space, c) }
          )
        end
      end

      def each_region(&block)
        return enum_for(:each_region) unless block_given?

        super(&block)

        yield Region.new(
          offset: pos_ibuf_header,
          length: size_ibuf_header,
          name: :ibuf_header,
          info: "Insert Buffer Header"
        )
      end

      def dump
        super

        puts "ibuf header:"
        pp ibuf_header
      end
    end
  end
end
