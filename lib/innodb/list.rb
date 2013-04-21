# An abstract InnoDB "free list" or FLST (renamed to just "list" here as it
# frequently is used for structures that aren't free lists). This class must
# be sub-classed to provide an appropriate #object_from_address method.
class Innodb::List
  # An "address", which consists of a page number and byte offset within the
  # page. This points to the list "node" pointers (prev and next) of the
  # node, not necessarily the node object.
  ADDRESS_SIZE = 4 + 2

  # Read a node address from a cursor. Return nil if the address is an end
  # or "NULL" pointer (the page number is UINT32_MAX), or the address if
  # valid.
  def self.get_address(cursor)
    page    = cursor.name("page") {
      Innodb::Page.maybe_undefined(cursor.get_uint32)
    }
    offset  = cursor.name("offset") { cursor.get_uint16 }
    if page
      {
        :page     => page,
        :offset   => offset,
      }
    end
  end

  # A list node consists of two addresses: the "previous" node address, and
  # the "next" node address.
  NODE_SIZE = 2 * ADDRESS_SIZE

  # Read a node, consisting of two consecutive addresses (:prev and :next)
  # from a cursor. Either address may be nil, indicating the end of the
  # linked list.
  def self.get_node(cursor)
    {
      :prev => cursor.name("prev") { get_address(cursor) },
      :next => cursor.name("next") { get_address(cursor) },
    }
  end

  # A list base node consists of a list length followed by two addresses:
  # the "first" node address, and the "last" node address.
  BASE_NODE_SIZE = 4 + (2 * ADDRESS_SIZE)

  # Read a base node, consisting of a list length followed by two addresses
  # (:first and :last) from a cursor. Either address may be nil. An empty list
  # has a :length of 0 and :first and :last are nil. A list with only a single
  # item will have a :length of 1 and :first and :last will point to the same
  # address.
  def self.get_base_node(cursor)
    {
      :length => cursor.name("length") { cursor.get_uint32 },
      :first  => cursor.name("first") { get_address(cursor) },
      :last   => cursor.name("last")  { get_address(cursor) },
    }
  end

  def initialize(space, base)
    @space  = space
    @base   = base
  end

  attr_reader :space
  attr_reader :base

  # Abstract #object_from_address method which must be implemented by
  # sub-classes in order to return a useful object given an object address.
  def object_from_address(address)
    raise "#{self.class} must implement object_from_address"
  end

  # Return the object pointed to by the "previous" address pointer of the
  # provided object.
  def prev(object)
    unless object.respond_to? :prev_address
      raise "Class #{object.class} does not respond to prev_address"
    end

    object_from_address(object.prev_address)
  end

  # Return the object pointed to by the "next" address pointer of the
  # provided object.
  def next(object)
    unless object.respond_to? :next_address
      raise "Class #{object.class} does not respond to next_address"
    end

    object_from_address(object.next_address)
  end

  # Return the first object in the list using the list base node "first"
  # address pointer.
  def first
    object_from_address(@base[:first])
  end

  # Return the first object in the list using the list base node "last"
  # address pointer.
  def last
    object_from_address(@base[:last])
  end

  # Return a list cursor for the list.
  def list_cursor(node=nil)
    ListCursor.new(self, node)
  end

  # Iterate through all nodes in the list.
  def each
    unless block_given?
      return enum_for(:each)
    end

    c = list_cursor
    while e = c.next
      yield e
    end
  end

  # A list iteration cursor used primarily by the Innodb::List #cursor method
  # implicitly. Keeps its own state for iterating through lists efficiently.
  class ListCursor
    def initialize(list, node=nil)
      @list   = list
      @cursor = node
    end

    # Reset the list cursor to its default starting state, which will allow
    # iteration forwards from the first entry (using #next) or backwards
    # from the last entry (using #prev).
    def reset
      @cursor = nil
    end

    # Return the previous entry from the current position, and advance the
    # cursor position to the returned entry. If the cursor is currently nil,
    # return the last entry in the list and adjust the cursor position to
    # that entry.
    def prev
      if @cursor
        @cursor = @list.prev(@cursor)
      else
        @cursor = @list.last
      end
    end

    # Return the next entry from the current position, and advance the
    # cursor position to the returned entry. If the cursor is currently nil,
    # return the first entry in the list and adjust the cursor position to
    # that entry.
    def next
      if @cursor
        @cursor = @list.next(@cursor)
      else
        @cursor = @list.first
      end
    end
  end
end

# A list of extent descriptor entries. Objects returned by list methods
# will be Innodb::Xdes objects.
class Innodb::List::Xdes < Innodb::List
  def object_from_address(address)
    if address && page = @space.page(address[:page])
      Innodb::Xdes.new(page, page.cursor(address[:offset] - 8))
    end
  end
end

# A list of Inode pages. Objects returned by list methods will be
# Innodb::Page::Inode objects.
class Innodb::List::Inode < Innodb::List
  def object_from_address(address)
    if address && page = @space.page(address[:page])
      page
    end
  end
end