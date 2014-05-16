# -*- encoding : utf-8 -*-

class Innodb::Page::IbufBitmap < Innodb::Page
  extend ReadBitsAtOffset

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
    unless block_given?
      return enum_for(:each_region)
    end

    super do |region|
      yield region
    end

    yield({
      :offset => pos_ibuf_bitmap,
      :length => size_ibuf_bitmap,
      :name => :ibuf_bitmap,
      :info => "Insert Buffer Bitmap",
    })

    nil
  end

  def dump
    super

    puts "ibuf bitmap:"
    ibuf_bitmap.each_page_status do |page_number, page_status|
      puts "  Page %i: %s" % [page_number, page_status.inspect]
    end
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:IBUF_BITMAP] = Innodb::Page::IbufBitmap
