# frozen_string_literal: true

module Innodb
  class Sdi
    class TableColumn
      HIDDEN_STATUSES = {
        VISIBLE: 1,
        HIDDEN_SE: 2,
        HIDDEN_SQL: 3,
        HIDDEN_USER: 4,
      }.freeze

      HIDDEN_STATUSES_BY_VALUE = HIDDEN_STATUSES.invert

      attr_reader :table
      attr_reader :data

      def initialize(table, data)
        @table = table
        @data = data
      end

      def name
        data["name"]
      end

      def type
        data["type"]
      end

      def nullable?
        data["is_nullable"]
      end

      def zerofill?
        data["is_zerofill"]
      end

      def unsigned?
        data["is_unsigned"]
      end

      def auto_increment?
        data["is_auto_increment"]
      end

      def virtual?
        data["is_virtual"]
      end

      def explicit_collation?
        data["is_explicit_collation"]
      end

      def hidden_status
        HIDDEN_STATUSES_BY_VALUE[data["hidden"]]
      end

      def visible?
        hidden_status != :VISIBLE
      end

      def hidden?
        !visible?
      end

      def system?
        %w[DB_TRX_ID DB_ROLL_PTR].include?(name)
      end

      def description
        [
          data["column_type_utf8"].sub(/ unsigned$/, ""),
          unsigned? ? :UNSIGNED : nil,
          nullable? ? nil : :NOT_NULL,
        ].compact
      end

      def se_private_data
        Innodb::Sdi.parse_se_private_data(data["se_private_data"])
      end

      def table_id
        se_private_data["table_id"].to_i
      end
    end
  end
end
