# -*- encoding : utf-8 -*-

class SimpleDescriber < Innodb::RecordDescriber
  type :clustered
  key "i", :INT, :NOT_NULL
  row "s", "VARCHAR(100)", :NOT_NULL
end
