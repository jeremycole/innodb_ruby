# frozen_string_literal: true

class SimpleDescriber < Innodb::RecordDescriber
  type :clustered
  key 'i', :INT, :NOT_NULL
  row 's', 'VARCHAR(100)', :NOT_NULL
end
