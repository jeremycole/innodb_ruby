lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)
require "innodb/version"

Gem::Specification.new do |s|
  s.name        = 'innodb_ruby'
  s.version     = Innodb::VERSION
  s.date        = Time.now.strftime("%Y-%m-%d")
  s.summary     = 'InnoDB data file parser'
  s.description = 'Library for parsing InnoDB data files in Ruby'
  s.authors     = [ 'Jeremy Cole' ]
  s.email       = 'jeremy@jcole.us'
  s.homepage    = 'http://jcole.us/'
  s.files = [
    'lib/innodb.rb',
    'lib/innodb/cursor.rb',
    'lib/innodb/free_list.rb',
    'lib/innodb/fseg_entry.rb',
    'lib/innodb/index.rb',
    'lib/innodb/log.rb',
    'lib/innodb/log_block.rb',
    'lib/innodb/page.rb',
    'lib/innodb/page/fsp_hdr_xdes.rb',
    'lib/innodb/page/index.rb',
    'lib/innodb/page/inode.rb',
    'lib/innodb/page/trx_sys.rb',
    'lib/innodb/record_describer.rb',
    'lib/innodb/space.rb',
    'lib/innodb/version.rb',
  ]
  s.executables = [
    'innodb_log',
    'innodb_space',
  ]
end
