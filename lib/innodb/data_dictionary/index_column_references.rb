# frozen_string_literal: true

require "innodb/data_dictionary/object_store"
require "innodb/data_dictionary/index_column_reference"

module Innodb
  class DataDictionary
    class IndexColumnReferences < ObjectStore
      def initialize
        super(allowed_type: IndexColumnReference)
      end
    end
  end
end
