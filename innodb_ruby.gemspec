# frozen_string_literal: true

lib = File.expand_path("lib", __dir__)
$LOAD_PATH.unshift lib unless $LOAD_PATH.include?(lib)
require "innodb/version"

Gem::Specification.new do |s|
  s.name        = "innodb_ruby"
  s.version     = Innodb::VERSION
  s.summary     = "InnoDB data file parser"
  s.license     = "BSD-3-Clause"
  s.description = "Library for parsing InnoDB data files in Ruby"
  s.authors     = [
    "Jeremy Cole",
    "Davi Arnaut",
  ]
  s.email       = "jeremy@jcole.us"
  s.homepage    = "https://github.com/jeremycole/innodb_ruby"
  s.files = Dir.glob("{bin,lib}/**/*") + %w[LICENSE AUTHORS.md README.md]
  s.executables = %w[innodb_log innodb_space]

  s.required_ruby_version = ">= 2.6"

  s.add_runtime_dependency("bindata", ">= 1.4.5", "< 3.0")
  s.add_runtime_dependency("digest-crc", "~> 0.4", ">= 0.4.1")
  s.add_runtime_dependency("histogram", "~> 0.2")

  s.add_development_dependency("gnuplot", "~> 2.6.0")
  s.add_development_dependency("rspec", "~> 3.11.0")
  s.add_development_dependency("rubocop", "~> 1.18")
  s.add_development_dependency("rubocop-rspec", "~> 2.4")
end
