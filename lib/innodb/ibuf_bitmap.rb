# -*- encoding : utf-8 -*-

class Innodb::IbufBitmap
  BITS_PER_PAGE = 4

  BITMAP_BV_FREE     = 1 + 2
  BITMAP_BV_BUFFERED = 4
  BITMAP_BV_IBUF     = 8

  BITMAP_BV_ALL =
    BITMAP_BV_FREE |
    BITMAP_BV_BUFFERED |
    BITMAP_BV_IBUF

  def initialize(page, cursor)
    @page = page
    @bitmap = read_bitmap(page, cursor)
  end

  def size_bitmap
    (@page.space.pages_per_bookkeeping_page * BITS_PER_PAGE) / 8
  end

  def read_bitmap(page, cursor)
    cursor.name("ibuf_bitmap") do |c|
      c.get_bytes(size_bitmap)
    end
  end

  def each_page_status
    unless block_given?
      return enum_for(:each_page_status)
    end

    bitmap = @bitmap.enum_for(:each_byte)

    bitmap.each_with_index do |byte, byte_index|
      (0..1).each do |page_offset|
        page_number = (byte_index * 2) + page_offset
        page_bits = ((byte >> (page_offset * BITS_PER_PAGE)) & BITMAP_BV_ALL)
        page_status = {
          :free => (page_bits & BITMAP_BV_FREE),
          :buffered => (page_bits & BITMAP_BV_BUFFERED != 0),
          :ibuf => (page_bits & BITMAP_BV_IBUF != 0),
        }
        yield page_number, page_status
      end
    end
  end
end
