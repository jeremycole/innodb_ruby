class Innodb::Page
  class Cursor
    attr_reader :offset

    def initialize(page, offset)
      @page = page
      @cursor = offset
      @direction = :forward
    end

    def forward
      @direction = :forward
      self
    end

    def backward
      @direction = :backward
      self
    end

    def seek(offset)
      @cursor = offset if offset
      @cursor
    end

    def adjust(relative_offset)
      @cursor += relative_offset
      @cursor
    end

    def read_and_advance(length, type=nil)
      data = nil
      case @direction
      when :forward
        data = @page.data(@cursor, length)
        adjust(length)
      when :backward
        adjust(-length)
        data = @page.data(@cursor, length)
      end
      type ? data.unpack(type).first : data
    end

    def get_bytes(length)
      read_and_advance(length)
    end

    def get_uint8(offset=nil)
      seek(offset)
      read_and_advance(1, "C")
    end

    def get_uint16(offset=nil)
      seek(offset)
      read_and_advance(2, "n")
    end

    def get_sint16(offset=nil)
      seek(offset)
      uint = read_and_advance(2, "n")
      (uint & 32768) == 0 ? uint : -(uint ^ 65535) - 1
    end
  
    def get_uint32(offset=nil)
      seek(offset)
      read_and_advance(4, "N")
    end
  
    def get_uint64(offset=nil)
      seek(offset)
      # Ruby 1.8 doesn't support big-endian quad-word unpack; unpack as two
      # 32-bit big-endian instead.
      high, low = read_and_advance(8).unpack("NN")
      (high << 32) | low
    end
  end
end