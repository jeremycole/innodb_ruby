# frozen_string_literal: true

module Innodb
  class DataType
    class Blob < DataType
      specialization_for :TINYBLOB
      specialization_for :BLOB
      specialization_for :MEDIUMBLOB
      specialization_for :LONGBLOB
      specialization_for :TINYTEXT
      specialization_for :TEXT
      specialization_for :MEDIUMTEXT
      specialization_for :LONGTEXT
      specialization_for :JSON
      specialization_for :GEOMETRY

      include HasNumericModifiers

      def variable?
        true
      end

      def blob?
        true
      end
    end
  end
end
