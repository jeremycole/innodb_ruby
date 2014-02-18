# -*- encoding : utf-8 -*-

require 'buffer_cursor'

# Monkey patch BufferCursor to support InnoDB-compressed 32-bit integers.
class BufferCursor
  # Read an InnoDB-compressed unsigned 32-bit integer.
  def get_ic_uint32
    flag = peek { name("ic_uint32") { get_uint8 } }

    case
    when flag < 0x80
      name("uint8") { get_uint8 }
    when flag < 0xc0
      name("uint16") { get_uint16 } & 0x7fff
    when flag < 0xe0
      name("uint24") { get_uint24 } & 0x3fffff
    when flag < 0xf0
      name("uint32") { get_uint32 } & 0x1fffffff
    when flag == 0xf0
      adjust(+1) # Skip the flag.
      name("uint32+1") { get_uint32 }
    else
      raise "Invalid flag #{flag.to_s} seen"
    end
  end
end
