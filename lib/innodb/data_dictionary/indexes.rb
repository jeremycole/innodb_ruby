# frozen_string_literal: true

require "innodb/data_dictionary/object_store"
require "innodb/data_dictionary/index"

module Innodb
  class DataDictionary
    class Indexes < ObjectStore
      def initialize
        super(allowed_type: Index)
      end
    end
  end
end
