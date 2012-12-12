class Innodb::Xdes
  PAGES_PER_EXTENT = 64
  BITS_PER_PAGE = 2
  BITMAP_SIZE = (PAGES_PER_EXTENT * BITS_PER_PAGE) / 8
  ENTRY_SIZE = 8 + Innodb::List::NODE_SIZE + 4 + BITMAP_SIZE

  STATES = {
    1 => :free,
    2 => :free_frag,
    3 => :full_frag,
    4 => :fseg,
  }

  def initialize(page, cursor)
    @page = page
    extent_number = (cursor.position - page.pos_xdes_array) / ENTRY_SIZE
    start_page = page.offset + (extent_number * PAGES_PER_EXTENT)
    @xdes = {
      :start_page => start_page,
      :fseg_id    => cursor.get_uint64,
      :this       => {:page => page.offset, :offset => cursor.position},
      :list       => Innodb::List.get_node(cursor),
      :state      => STATES[cursor.get_uint32],
      :bitmap     => cursor.get_hex(BITMAP_SIZE),
    }
  end

  def xdes
    @xdes
  end

  def prev_address
    @xdes[:list][:prev]
  end

  def next_address
    @xdes[:list][:next]
  end
end