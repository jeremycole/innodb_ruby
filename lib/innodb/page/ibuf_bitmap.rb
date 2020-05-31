# frozen_string_literal: true

module Innodb
  class Page
    class IbufBitmap < Page
      extend ReadBitsAtOffset

      specialization_for :IBUF_BITMAP

      def pos_ibuf_bitmap
        pos_page_body
      end

      def size_ibuf_bitmap
        (Innodb::IbufBitmap::BITS_PER_PAGE * space.pages_per_bookkeeping_page) / 8
      end

      def ibuf_bitmap
        Innodb::IbufBitmap.new(self, cursor(pos_ibuf_bitmap))
      end

      def each_region
        return enum_for(:each_region) unless block_given?

        super do |region|
          yield region
        end

        yield({
          offset: pos_ibuf_bitmap,
          length: size_ibuf_bitmap,
          name: :ibuf_bitmap,
          info: 'Insert Buffer Bitmap',
        })

        nil
      end

      def dump
        super

        puts 'ibuf bitmap:'
        ibuf_bitmap.each_page_status do |page_number, page_status|
          puts '  Page %i: %s' % [page_number, page_status.inspect]
        end
      end
    end
  end
end
