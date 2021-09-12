# frozen_string_literal: true

# An abstract InnoDB "free list" or FLST (renamed to just "list" here as it
# frequently is used for structures that aren't free lists). This class must
# be sub-classed to provide an appropriate #object_from_address method.

module Innodb
  class List
    BaseNode = Struct.new(
      :length, # rubocop:disable Lint/StructNewOverride
      :first, # rubocop:disable Lint/StructNewOverride
      :last,
      keyword_init: true
    )

    Node = Struct.new(
      :prev,
      :next,
      keyword_init: true
    )

    # An "address", which consists of a page number and byte offset within the
    # page. This points to the list "node" pointers (prev and next) of the
    # node, not necessarily the node object.
    ADDRESS_SIZE = 4 + 2

    # Read a node address from a cursor. Return nil if the address is an end
    # or "NULL" pointer (the page number is UINT32_MAX), or the address if
    # valid.
    def self.get_address(cursor)
      page = cursor.name("page") { Innodb::Page.maybe_undefined(cursor.read_uint32) }
      offset = cursor.name("offset") { cursor.read_uint16 }

      Innodb::Page::Address.new(page: page, offset: offset) if page
    end

    # A list node consists of two addresses: the "previous" node address, and
    # the "next" node address.
    NODE_SIZE = 2 * ADDRESS_SIZE

    # Read a node, consisting of two consecutive addresses (:prev and :next)
    # from a cursor. Either address may be nil, indicating the end of the
    # linked list.
    def self.get_node(cursor)
      Node.new(
        prev: cursor.name("prev") { get_address(cursor) },
        next: cursor.name("next") { get_address(cursor) }
      )
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
      BaseNode.new(
        length: cursor.name("length") { cursor.read_uint32 },
        first: cursor.name("first") { get_address(cursor) },
        last: cursor.name("last") { get_address(cursor) }
      )
    end

    attr_reader :space
    attr_reader :base

    def initialize(space, base)
      @space = space
      @base = base
    end

    # Abstract #object_from_address method which must be implemented by
    # sub-classes in order to return a useful object given an object address.
    def object_from_address(_address)
      raise "#{self.class} must implement object_from_address"
    end

    # Return the object pointed to by the "previous" address pointer of the
    # provided object.
    def prev(object)
      raise "Class #{object.class} does not respond to prev_address" unless object.respond_to?(:prev_address)

      object_from_address(object.prev_address)
    end

    # Return the object pointed to by the "next" address pointer of the
    # provided object.
    def next(object)
      raise "Class #{object.class} does not respond to next_address" unless object.respond_to?(:next_address)

      object_from_address(object.next_address)
    end

    # Return the number of items in the list.
    def length
      @base.length
    end

    # Is the list currently empty?
    def empty?
      length.zero?
    end

    # Return the first object in the list using the list base node "first"
    # address pointer.
    def first
      object_from_address(@base.first)
    end

    # Return the first object in the list using the list base node "last"
    # address pointer.
    def last
      object_from_address(@base.last)
    end

    # Return a list cursor for the list.
    def list_cursor(node = :min, direction = :forward)
      ListCursor.new(self, node, direction)
    end

    # Return whether the given item is present in the list. This depends on the
    # item and the items in the list implementing some sufficient == method.
    # This is implemented rather inefficiently by constructing an array and
    # leaning on Array#include? to do the real work.
    def include?(item)
      each.to_a.include?(item)
    end

    # Iterate through all nodes in the list.
    def each(&block)
      return enum_for(:each) unless block_given?

      list_cursor.each_node(&block)
    end

    # A list iteration cursor used primarily by the Innodb::List #cursor method
    # implicitly. Keeps its own state for iterating through lists efficiently.
    class ListCursor
      def initialize(list, node = :min, direction = :forward)
        @initial = true
        @list = list
        @direction = direction
        @node = initial_node(node)
      end

      def initial_node(node)
        case node
        when :min
          @list.first
        when :max
          @list.last
        else
          node
        end
      end

      def node
        if @initial
          @initial = false
          return @node
        end

        case @direction
        when :forward
          next_node
        when :backward
          prev_node
        end
      end

      def goto_node(node)
        @node = node if node
      end

      # Return the previous entry from the current position, and advance the
      # cursor position to the returned entry. If the cursor is currently nil,
      # return the last entry in the list and adjust the cursor position to
      # that entry.
      def prev_node
        goto_node(@list.prev(@node))
      end

      # Return the next entry from the current position, and advance the
      # cursor position to the returned entry. If the cursor is currently nil,
      # return the first entry in the list and adjust the cursor position to
      # that entry.
      def next_node
        goto_node(@list.next(@node))
      end

      def each_node
        return enum_for(:each_node) unless block_given?

        while (n = node)
          yield n
        end
      end
    end

    # A list of extent descriptor entries. Objects returned by list methods
    # will be Innodb::Xdes objects.
    class Xdes < Innodb::List
      def object_from_address(address)
        return unless address

        page = @space.page(address.page)
        return unless page

        Innodb::Xdes.new(page, page.cursor(address.offset - 8))
      end
    end

    # A list of Inode pages. Objects returned by list methods will be
    # Innodb::Page::Inode objects.
    class Inode < Innodb::List
      def object_from_address(address)
        return unless address

        @space.page(address.page)
      end
    end

    class UndoPage < Innodb::List
      def object_from_address(address)
        return unless address

        @space.page(address.page)
      end
    end

    class History < Innodb::List
      def object_from_address(address)
        return unless address

        page = @space.page(address.page)
        return unless page

        Innodb::UndoLog.new(page, address.offset - 34)
      end
    end
  end
end
