# frozen_string_literal: true

require "innodb/data_dictionary/object_store"
require "innodb/data_dictionary/table"

module Innodb
  class DataDictionary
    class Tables < ObjectStore
      def initialize
        super(allowed_type: Table)
      end
    end
  end
end
