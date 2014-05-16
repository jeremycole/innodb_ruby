# -*- encoding : utf-8 -*-

class Innodb::Page::SysIbufHeader < Innodb::Page
  def pos_ibuf_header
    pos_page_body
  end

  def size_ibuf_header
    Innodb::FsegEntry::SIZE
  end

  def ibuf_header
    cursor(pos_ibuf_header).name("ibuf_header") do |c|
      {
        :fseg => c.name("fseg") {
          Innodb::FsegEntry.get_inode(space, c)
        }
      }
    end
  end

  def each_region
    unless block_given?
      return enum_for(:each_region)
    end

    super do |region|
      yield region
    end

    yield({
      :offset => pos_ibuf_header,
      :length => size_ibuf_header,
      :name => :ibuf_header,
      :info => "Insert Buffer Header",
    })
  end

  def dump
    super

    puts "ibuf header:"
    pp ibuf_header
  end
end
