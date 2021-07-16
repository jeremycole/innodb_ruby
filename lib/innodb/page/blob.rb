# frozen_string_literal: true

module Innodb
  class Page
    class Blob < Page
      specialization_for :BLOB

      def pos_blob_header
        pos_page_body
      end

      def size_blob_header
        4 + 4
      end

      def pos_blob_data
        pos_blob_header + size_blob_header
      end

      def blob_header
        cursor(pos_blob_header).name('blob_header') do |c|
          {
            length: c.name('length') { c.read_uint32 },
            next: c.name('next') { Innodb::Page.maybe_undefined(c.read_uint32) },
          }
        end
      end

      def blob_data
        cursor(pos_blob_data).name('blob_data') do |c|
          c.read_bytes(blob_header[:length])
        end
      end

      def dump_hex(string)
        slice_size = 16
        string.chars.each_slice(slice_size).each_with_index do |slice_bytes, slice_count|
          puts '%08i  %-23s  %-23s  |%-16s|' % [
            (slice_count * slice_size),
            slice_bytes[0..8].map { |n| '%02x' % n.ord }.join(' '),
            slice_bytes[8..16].map { |n| '%02x' % n.ord }.join(' '),
            slice_bytes.join,
          ]
        end
      end

      def each_region(&block)
        return enum_for(:each_region) unless block_given?

        super(&block)

        yield Region.new(
          offset: pos_blob_header,
          length: size_blob_header,
          name: :blob_header,
          info: 'Blob Header'
        )

        yield Region.new(
          offset: pos_blob_data,
          length: blob_header[:length],
          name: :blob_data,
          info: 'Blob Data'
        )

        nil
      end

      # Dump the contents of a page for debugging purposes.
      def dump
        super

        puts 'blob header:'
        pp blob_header
        puts

        puts 'blob data:'
        dump_hex(blob_data)
        puts

        puts
      end
    end
  end
end
