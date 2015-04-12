lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)
require "innodb/version"

Gem::Specification.new do |s|
  s.name        = 'innodb_ruby'
  s.version     = Innodb::VERSION
  s.date        = Time.now.strftime("%Y-%m-%d")
  s.summary     = 'InnoDB data file parser'
  s.license     = 'New BSD (3-clause)'
  s.description = 'Library for parsing InnoDB data files in Ruby'
  s.authors     = [
    'Jeremy Cole',
    'Davi Arnaut',
  ]
  s.email       = 'jeremy@jcole.us'
  s.homepage    = 'https://github.com/jeremycole/innodb_ruby'
  s.files = [
    'LICENSE',
    'AUTHORS.md',
    'README.md',
    'lib/innodb.rb',
    'lib/innodb/checksum.rb',
    'lib/innodb/data_dictionary.rb',
    'lib/innodb/data_type.rb',
    'lib/innodb/field.rb',
    'lib/innodb/fseg_entry.rb',
    'lib/innodb/history.rb',
    'lib/innodb/history_list.rb',
    'lib/innodb/ibuf_bitmap.rb',
    'lib/innodb/ibuf_index.rb',
    'lib/innodb/index.rb',
    'lib/innodb/inode.rb',
    'lib/innodb/list.rb',
    'lib/innodb/lsn.rb',
    'lib/innodb/log.rb',
    'lib/innodb/log_block.rb',
    'lib/innodb/log_group.rb',
    'lib/innodb/log_record.rb',
    'lib/innodb/log_reader.rb',
    'lib/innodb/page.rb',
    'lib/innodb/page/blob.rb',
    'lib/innodb/page/fsp_hdr_xdes.rb',
    'lib/innodb/page/ibuf_bitmap.rb',
    'lib/innodb/page/index.rb',
    'lib/innodb/page/index_uncompressed.rb',
    'lib/innodb/page/index_compressed.rb',
    'lib/innodb/page/inode.rb',
    'lib/innodb/page/sys.rb',
    'lib/innodb/page/sys_data_dictionary_header.rb',
    'lib/innodb/page/sys_ibuf_header.rb',
    'lib/innodb/page/sys_rseg_header.rb',
    'lib/innodb/page/trx_sys.rb',
    'lib/innodb/page/undo_log.rb',
    'lib/innodb/record.rb',
    'lib/innodb/record_describer.rb',
    'lib/innodb/space.rb',
    'lib/innodb/stats.rb',
    'lib/innodb/system.rb',
    'lib/innodb/undo_log.rb',
    'lib/innodb/undo_record.rb',
    'lib/innodb/util/buffer_cursor.rb',
    'lib/innodb/util/read_bits_at_offset.rb',
    'lib/innodb/version.rb',
    'lib/innodb/xdes.rb',
  ]
  s.executables = [
    'innodb_log',
    'innodb_space',
  ]
  s.add_dependency('bindata', '>= 1.4.5')
end
