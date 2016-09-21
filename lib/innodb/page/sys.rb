# -*- encoding : utf-8 -*-

require "innodb/page/sys_rseg_header"
require "innodb/page/sys_data_dictionary_header"
require "innodb/page/sys_ibuf_header"

# Another layer of indirection for pages of type SYS, as they have multiple
# uses within InnoDB. We'll override the self.handle method and check the
# page offset to decide which type of SYS page this is.
class Innodb::Page::Sys < Innodb::Page
  def self.handle(page, space, buffer, page_number=nil)
    case
    when page.offset == 3
      Innodb::Page::SysIbufHeader.new(space, buffer, page_number)
    when page.offset == 7
      Innodb::Page::SysDataDictionaryHeader.new(space, buffer, page_number)
    when space.rseg_page?(page.offset)
      Innodb::Page::SysRsegHeader.new(space, buffer, page_number)
    else
      # We can't do anything better, so pass on the generic InnoDB::Page.
      page
    end
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:SYS] = Innodb::Page::Sys
