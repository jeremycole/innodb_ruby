# frozen_string_literal: true

module Innodb
  class Page
    # SDI (Serialized Dictionary Information) BLOB pages are actually BLOB pages with a different page
    # type number but otherwise the same structure.
    class SdiBlob < Blob
      specialization_for :SDI_BLOB
    end
  end
end
