# A specialized class for TRX_SYS pages, which contain various information
# about the transaction system within InnoDB. Only one TRX_SYS page exists in
# any given InnoDB installation, and it is page 5 of the system tablespace
# (space 0), most commonly named "ibdata1".
#
# The basic structure of a TRX_SYS page is: FIL header, TRX_SYS header,
# empty space, master binary log information, empty space, local binary
# log information, empty space, doublewrite information (repeated twice),
# empty space, and FIL trailer.
class Innodb::Page::TrxSys < Innodb::Page
  # The TRX_SYS header immediately follows the FIL header.
  def pos_trx_sys_header
    pos_fil_header + size_fil_header
  end

  # The master's binary log information is located 2000 bytes from the end of
  # the page.
  def pos_mysql_master_log_info
    size - 2000
  end

  # The local binary log information is located 1000 bytes from the end of
  # the page.
  def pos_mysql_binary_log_info
    size - 1000
  end

  # The doublewrite buffer information is located 200 bytes from the end of
  # the page.
  def pos_doublewrite_info
    size - 200
  end

  # A magic number present in each MySQL binary log information structure,
  # which helps identify whether the structure is populated or not.
  MYSQL_LOG_MAGIC_N = 873422344

  # Read a MySQL binary log information structure from a given position.
  def mysql_log_info(cursor, offset)
    cursor.peek(offset) do |c|
      if c.name("magic_n") { c.get_uint32 } == MYSQL_LOG_MAGIC_N
        {
          :offset => c.name("offset") { c.get_uint64 },
          :name   => c.name("name")   { c.get_bytes(100) },
        }
      end
    end
  end

  # A magic number present in each doublewrite buffer information structure,
  # which helps identify whether the structure is populated or not.
  DOUBLEWRITE_MAGIC_N = 536853855

  # Read a single doublewrite buffer information structure from a given cursor.
  def doublewrite_page_info(cursor)
    {
      :magic_n => cursor.name("magic_n") { cursor.get_uint32 },
      :page_number => [
        cursor.name("page[0]") { cursor.get_uint32 },
        cursor.name("page[1]") { cursor.get_uint32 },
      ],
    }
  end

  # A magic number present in the overall doublewrite buffer structure,
  # which identifies whether the space id is stored.
  DOUBLEWRITE_SPACE_ID_STORED_MAGIC_N = 1783657386

  # Read the overall doublewrite buffer structures
  def doublewrite_info(cursor)
    cursor.peek(pos_doublewrite_info) do |c_doublewrite|
      c_doublewrite.name("doublewrite") do |c|
        {
          :fseg => c.name("fseg") { Innodb::FsegEntry.get_inode(@space, c) },
          :page_info => [
            c.name("group[0]") { doublewrite_page_info(c) },
            c.name("group[1]") { doublewrite_page_info(c) },
          ],
          :space_id_stored =>
            (c.name("space_id_stored") { c.get_uint32 } ==
               DOUBLEWRITE_SPACE_ID_STORED_MAGIC_N),
        }
      end
    end
  end

  # Read the TRX_SYS headers and other information.
  def trx_sys
    @trx_sys ||= cursor(pos_trx_sys_header).name("trx_sys") do |c|
      {
        :trx_id       => c.name("trx_id") { c.get_uint64 },
        :fseg         => c.name("fseg") {
          Innodb::FsegEntry.get_inode(@space, c)
        },
        :binary_log   => c.name("binary_log") {
          mysql_log_info(c, pos_mysql_binary_log_info)
        },
        :master_log   => c.name("master_log") {
          mysql_log_info(c, pos_mysql_master_log_info)
        },
        :doublewrite  => doublewrite_info(c),
      }
    end
  end

  def trx_id;       trx_sys[:trx_id];       end
  def fseg;         trx_sys[:fseg];         end
  def binary_log;   trx_sys[:binary_log];   end
  def master_log;   trx_sys[:master_log];   end
  def doublewrite;  trx_sys[:doublewrite];  end

  # Dump the contents of a page for debugging purposes.
  def dump
    super

    puts "trx_sys:"
    pp trx_sys
    puts
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:TRX_SYS] = Innodb::Page::TrxSys
