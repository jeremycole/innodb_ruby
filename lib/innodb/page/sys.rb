# frozen_string_literal: true

require "innodb/page/sys_rseg_header"
require "innodb/page/sys_data_dictionary_header"
require "innodb/page/sys_ibuf_header"

# Another layer of indirection for pages of type SYS, as they have multiple
# uses within InnoDB. We'll override the self.handle method and check the
# page offset to decide which type of SYS page this is.
module Innodb
  class Page
    class Sys < Page
      specialization_for :SYS

      def self.handle(page, space, buffer, page_number = nil)
        return Innodb::Page::SysIbufHeader.new(space, buffer, page_number) if page.offset == 3
        return Innodb::Page::SysDataDictionaryHeader.new(space, buffer, page_number) if page.offset == 7
        return Innodb::Page::SysRsegHeader.new(space, buffer, page_number) if space.rseg_page?(page.offset)

        page
      end
    end
  end
end
