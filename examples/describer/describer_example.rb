class Innodb::RecordDescriber::Example < Innodb::RecordDescriber
  def self.cursor_sendable_description(page)
    {
      :type => :clustered,
      :key => [
        [:INT, :UNSIGNED, :NOT_NULL],
      ],
      :row => [
        [:TINYINT],
        [:SMALLINT],
        [:MEDIUMINT],
        [:INT],
        [:BIGINT],
        [:TINYINT,  :UNSIGNED],
        [:SMALLINT, :UNSIGNED],
        [:MEDIUMINT,:UNSIGNED],
        [:INT,      :UNSIGNED],
        [:BIGINT,   :UNSIGNED],
      ],
    }
  end
end
