class Innodb::RecordDescriber::Example < Innodb::RecordDescriber
  def self.cursor_sendable_description(page)
    {
      :type => :clustered,
      :key => [
        [:get_uint32],
      ],
      :row => [
        [:get_i_sint8],   # TINYINT SIGNED
        [:get_i_sint16],  # SMALLINT SIGNED
        [:get_i_sint24],  # MEDIUMINT SIGNED
        [:get_i_sint32],  # INT SIGNED
        [:get_i_sint64],  # BIGINT SIGNED
        [:get_uint8],     # TINYINT UNSIGNED
        [:get_uint16],    # SMALLINT UNSIGNED
        [:get_uint24],    # MEDIUMINT UNSIGNED
        [:get_uint32],    # INT UNSIGNED
        [:get_uint64],    # BIGINT UNSIGNED

      ],
    }
  end
end
