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
    'lib/innodb/buffer_cursor.rb',
    'lib/innodb/checksum.rb',
    'lib/innodb/data_dictionary.rb',
    'lib/innodb/data_type.rb',
    'lib/innodb/field.rb',
    'lib/innodb/fseg_entry.rb',
    'lib/innodb/index.rb',
    'lib/innodb/inode.rb',
    'lib/innodb/list.rb',
    'lib/innodb/log.rb',
    'lib/innodb/log_block.rb',
    'lib/innodb/log_group.rb',
    'lib/innodb/page.rb',
    'lib/innodb/page/blob.rb',
    'lib/innodb/page/fsp_hdr_xdes.rb',
    'lib/innodb/page/index.rb',
    'lib/innodb/page/index_compressed.rb',
    'lib/innodb/page/inode.rb',
    'lib/innodb/page/sys.rb',
    'lib/innodb/page/sys_data_dictionary_header.rb',
    'lib/innodb/page/sys_rseg_header.rb',
    'lib/innodb/page/trx_sys.rb',
    'lib/innodb/page/undo_log.rb',
    'lib/innodb/record.rb',
    'lib/innodb/record_describer.rb',
    'lib/innodb/space.rb',
    'lib/innodb/system.rb',
    'lib/innodb/undo_log.rb',
    'lib/innodb/version.rb',
    'lib/innodb/xdes.rb',
  ]
  s.executables = [
    'innodb_log',
    'innodb_space',
  ]
  s.add_dependency('bindata', '>= 1.4.5')
  s.add_dependency('buffer_cursor', '>= 0.9.0')
end
