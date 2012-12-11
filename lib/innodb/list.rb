class Innodb::List
  FIL_ADDR_SIZE   = 4 + 2
  NODE_SIZE       = 2 * FIL_ADDR_SIZE
  BASE_NODE_SIZE  = 4 + (2 * FIL_ADDR_SIZE)

  def self.get_address(cursor)
    {
      :page     => Innodb::Page.maybe_undefined(cursor.get_uint32),
      :offset   => cursor.get_uint16,
    }
  end

  def self.get_node(cursor)
    {
      :prev => Innodb::List.get_address(cursor),
      :next => Innodb::List.get_address(cursor),
    }
  end

  def self.get_base_node(cursor)
    {
      :length => cursor.get_uint32,
      :first  => Innodb::List.get_address(cursor),
      :last   => Innodb::List.get_address(cursor),
    }
  end
end