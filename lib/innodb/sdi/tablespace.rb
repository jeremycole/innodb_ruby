# frozen_string_literal: true

module Innodb
  class Sdi
    class Tablespace < SdiObject
      specialization_for 2

      def space_id
        se_private_data["id"].to_i
      end
    end
  end
end
