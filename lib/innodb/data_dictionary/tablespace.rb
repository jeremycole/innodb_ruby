# frozen_string_literal: true

module Innodb
  class DataDictionary
    class Tablespace
      attr_reader :name
      attr_reader :innodb_space_id

      def initialize(name:, innodb_space_id:)
        @name = name
        @innodb_space_id = innodb_space_id
      end
    end
  end
end
