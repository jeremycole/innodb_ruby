# frozen_string_literal: true

require "innodb/data_dictionary/object_store"
require "innodb/data_dictionary/tablespace"

module Innodb
  class DataDictionary
    class Tablespaces < ObjectStore
      def initialize
        super(allowed_type: Tablespace)
      end
    end
  end
end
