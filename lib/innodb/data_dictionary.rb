class Innodb::DataDictionary
  # A record describer for SYS_TABLES clustered records.
  class SYS_TABLES_PRIMARY < Innodb::RecordDescriber
    type :clustered
    key "NAME",         "VARCHAR(100)",      :NOT_NULL
    row "ID",           :BIGINT, :UNSIGNED,  :NOT_NULL
    row "N_COLS",       :INT,    :UNSIGNED,  :NOT_NULL
    row "TYPE",         :INT,    :UNSIGNED,  :NOT_NULL
    row "MIX_ID",       :BIGINT, :UNSIGNED,  :NOT_NULL
    row "MIX_LEN",      :INT,    :UNSIGNED,  :NOT_NULL
    row "CLUSTER_NAME", "VARCHAR(100)",      :NOT_NULL
    row "SPACE",        :INT,    :UNSIGNED,  :NOT_NULL
  end

  # A record describer for SYS_TABLES secondary key on ID.
  class SYS_TABLES_ID < Innodb::RecordDescriber
    type :secondary
    key "ID",           :BIGINT, :UNSIGNED,  :NOT_NULL
    row "NAME",         "VARCHAR(100)",      :NOT_NULL
  end

  # A record describer for SYS_COLUMNS clustered records.
  class SYS_COLUMNS_PRIMARY < Innodb::RecordDescriber
    type :clustered
    key "TABLE_ID",     :BIGINT, :UNSIGNED,  :NOT_NULL
    key "POS",          :INT,    :UNSIGNED,  :NOT_NULL
    row "NAME",         "VARCHAR(100)",      :NOT_NULL
    row "MTYPE",        :INT,    :UNSIGNED,  :NOT_NULL
    row "PRTYPE",       :INT,    :UNSIGNED,  :NOT_NULL
    row "LEN",          :INT,    :UNSIGNED,  :NOT_NULL
    row "PREC",         :INT,    :UNSIGNED,  :NOT_NULL
  end

  # A record describer for SYS_INDEXES clustered records.
  class SYS_INDEXES_PRIMARY < Innodb::RecordDescriber
    type :clustered
    key "TABLE_ID",     :BIGINT, :UNSIGNED,  :NOT_NULL
    key "ID",           :BIGINT, :UNSIGNED,  :NOT_NULL
    row "NAME",         "VARCHAR(100)",      :NOT_NULL
    row "N_FIELDS",     :INT,    :UNSIGNED,  :NOT_NULL
    row "TYPE",         :INT,    :UNSIGNED,  :NOT_NULL
    row "SPACE",        :INT,    :UNSIGNED,  :NOT_NULL
    row "PAGE_NO",      :INT,    :UNSIGNED,  :NOT_NULL
  end

  # A record describer for SYS_FIELDS clustered records.
  class SYS_FIELDS_PRIMARY < Innodb::RecordDescriber
    type :clustered
    key "INDEX_ID",     :BIGINT, :UNSIGNED,  :NOT_NULL
    key "POS",          :INT,    :UNSIGNED,  :NOT_NULL
    row "COL_NAME",     "VARCHAR(100)",      :NOT_NULL
  end
end
