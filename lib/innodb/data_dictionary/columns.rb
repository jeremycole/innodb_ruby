# frozen_string_literal: true

require "innodb/data_dictionary/object_store"
require "innodb/data_dictionary/column"

module Innodb
  class DataDictionary
    class Columns < ObjectStore
      def initialize
        super(allowed_type: Column)
      end
    end
  end
end
