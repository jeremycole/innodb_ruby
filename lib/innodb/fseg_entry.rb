class Innodb::FsegEntry
  SIZE = 4 + 4 + 2

  def self.get_entry(cursor)
    {
      :space_id     => cursor.get_uint32,
      :page_number  => cursor.get_uint32,
      :offset       => cursor.get_uint16,
    }
  end
end