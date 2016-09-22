# -*- encoding : utf-8 -*-

require File.join(File.dirname(__FILE__), '..', 'lib', 'innodb')

RSpec.configure do |config|
  # Enable the below to allow easier fixing of deprecated RSpec syntax.
  #config.raise_errors_for_deprecations!
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end

# This mess is to deal with Ruby 1.8 to 1.9 transition breaking Enumerable.
def is_enumerator?(object)
  if defined?(Enumerator)
    object.instance_of?(Enumerator)
  elsif defined?(Enumerable::Enumerator)
    object.instance_of?(Enumerable::Enumerator)
  else
    false
  end
end
