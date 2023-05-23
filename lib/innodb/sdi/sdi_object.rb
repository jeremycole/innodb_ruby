# frozen_string_literal: true

require "zlib"
require "json"

module Innodb
  class Sdi
    class SdiObject
      class << self
        def specialization_for(id)
          Innodb::Sdi.register_specialization(id, self)
        end

        def from_record(record)
          type = record.key[0].value
          id = record.key[1].value
          # Ignore uncompressed_len = record.row[0].value
          # Ignore compressed_len = record.row[1].value
          data = record.full_value_with_externs_for_field(record.row[2])

          type_handler = Innodb::Sdi.specialized_classes[type]
          return unless type_handler

          parsed_data = JSON.parse(Zlib::Inflate.inflate(data))
          type_handler.new(type, id, parsed_data)
        end
      end

      attr_reader :type
      attr_reader :id
      attr_reader :data

      def initialize(type, id, data)
        @type = type
        @id = id
        @data = data
      end

      def mysqld_version_id
        data["mysqld_version_id"]
      end

      def dd_version
        data["dd_version"]
      end

      def sdi_version
        data["sdi_version"]
      end

      def dd_object_type
        data["dd_object_type"]
      end

      def dd_object
        data["dd_object"]
      end

      def name
        dd_object["name"]
      end

      def options
        data["options"]&.split(";").to_h { |x| x.split("=") }
      end

      def se_private_data
        data["se_private_data"]&.split(";").to_h { |x| x.split("=") }
      end
    end
  end
end
