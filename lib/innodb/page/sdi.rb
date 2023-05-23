# frozen_string_literal: true

module Innodb
  class Page
    # SDI (Serialized Dictionary Information) pages are actually INDEX pages and store data dictionary
    # information in an InnoDB index structure in the typical way. However they use a fixed definition
    # for the (unnamed except for in-memory as "SDI_<space_id>") index, since there would, logically,
    # be nowhere else to store the definition of this index.
    class Sdi < Index
      specialization_for :SDI

      # Every SDI index has the same structure, equivalent to the following SQL:
      #
      #   CREATE TABLE `SDI_<space_id>` (
      #     `type` INT UNSIGNED NOT NULL,
      #     `id` BIGINT UNSIGNED NOT NULL,
      #     `uncompressed_len` INT UNSIGNED NOT NULL,
      #     `compressed_len` INT UNSIGNED NOT NULL,
      #     `data` LONGBLOB NOT NULL,
      #     PRIMARY KEY (`type`, `id`)
      #   )
      #
      class RecordDescriber < Innodb::RecordDescriber
        type :clustered
        key "type",             :INT,    :UNSIGNED,  :NOT_NULL
        key "id",               :BIGINT, :UNSIGNED,  :NOT_NULL
        row "uncompressed_len", :INT,    :UNSIGNED,  :NOT_NULL
        row "compressed_len",   :INT,    :UNSIGNED,  :NOT_NULL
        row "data",             :BLOB,   :NOT_NULL
      end

      def make_record_describer
        RecordDescriber.new
      end
    end
  end
end
