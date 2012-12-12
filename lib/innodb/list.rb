class Innodb::List
  FIL_ADDR_SIZE   = 4 + 2
  NODE_SIZE       = 2 * FIL_ADDR_SIZE
  BASE_NODE_SIZE  = 4 + (2 * FIL_ADDR_SIZE)

  def self.get_address(cursor)
    page    = Innodb::Page.maybe_undefined(cursor.get_uint32)
    offset  = cursor.get_uint16
    if page
      {
        :page     => page,
        :offset   => offset,
      }
    end
  end

  def self.get_node(cursor)
    {
      :prev => get_address(cursor),
      :next => get_address(cursor),
    }
  end

  def self.get_base_node(cursor)
    {
      :length => cursor.get_uint32,
      :first  => get_address(cursor),
      :last   => get_address(cursor),
    }
  end

  def initialize(space, base)
    @space  = space
    @base   = base
  end

  attr_reader :space
  attr_reader :base

  def prev(object)
    object_from_address(object.prev_address)
  end

  def next(object)
    object_from_address(object.next_address)
  end

  def first
    object_from_address(@base[:first])
  end

  def last
    object_from_address(@base[:last])
  end

  def cursor(node=nil)
    Cursor.new(self, node)
  end

  def each
    c = cursor
    while e = c.next
      yield e
    end
  end

  class Cursor
    def initialize(list, node=nil)
      @list   = list
      @cursor = node
    end

    def reset
      @cursor = nil
    end

    def prev
      if @cursor
        @cursor = @list.prev(@cursor)
      else
        @cursor = @list.last
      end
    end

    def next
      if @cursor
        @cursor = @list.next(@cursor)
      else
        @cursor = @list.first
      end
    end
  end
end

class Innodb::List::Xdes < Innodb::List
  def object_from_address(address)
    if address && page = @space.page(address[:page])
      Innodb::Xdes.new(page, page.cursor(address[:offset] - 8))
    end
  end
end

class Innodb::List::Inode < Innodb::List
  def object_from_address(address)
    if address && page = @space.page(address[:page])
      page
    end
  end
end