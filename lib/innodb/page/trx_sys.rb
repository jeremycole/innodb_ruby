class Innodb::Page::TrxSys < Innodb::Page
  def pos_trx_sys_header
    pos_fil_header + size_fil_header
  end

  def pos_mysql_binary_log_info
    size - 1000
  end

  def pos_mysql_master_log_info
    size - 2000
  end

  def pos_doublewrite_info
    size - 200
  end

  MYSQL_LOG_MAGIC_N = 873422344

  def mysql_log_info(offset)
    c = cursor(offset)
    if c.get_uint32 == MYSQL_LOG_MAGIC_N
      {
        :offset => c.get_uint64,
        :name => c.get_bytes(100),
      }
    end
  end

  DOUBLEWRITE_MAGIC_N = 536853855

  def doublewrite_page_info(cursor)
    {
      :magic_n => cursor.get_uint32,
      :page_number => [
        cursor.get_uint32,
        cursor.get_uint32,
      ],
    }
  end

  DOUBLEWRITE_SPACE_ID_STORED_MAGIC_N = 1783657386

  def doublewrite_info
    c = cursor(pos_doublewrite_info)
    {
      :fseg => Innodb::FsegEntry.get_entry(c),
      :page_info => [
        doublewrite_page_info(c),
        doublewrite_page_info(c),
      ],
      :space_id_stored => c.get_uint32 == DOUBLEWRITE_SPACE_ID_STORED_MAGIC_N,
    }
  end

  def trx_sys
    c = cursor(pos_trx_sys_header)
    @trx_sys ||= {
      :trx_id            => c.get_uint64,
      :fseg        => Innodb::FsegEntry.get_entry(c),
      :binary_log => mysql_log_info(pos_mysql_binary_log_info),
      :master_log => mysql_log_info(pos_mysql_master_log_info),
      :doublewrite => doublewrite_info,
    }
  end

  def dump
    super

    puts "trx_sys:"
    pp trx_sys
    puts
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:TRX_SYS] = Innodb::Page::TrxSys