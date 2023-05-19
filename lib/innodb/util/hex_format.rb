# frozen_string_literal: true

module HexFormat
  LINE_SIZE = 16
  GROUP_SIZE = 8
  GROUP_FORMAT_LENGTH = ((LINE_SIZE.to_f / GROUP_SIZE).ceil * (GROUP_SIZE * 3))

  def self.format_group(data)
    data.map { |n| "%02x" % n.ord }.join(" ")
  end

  def self.format_groups(data, size)
    data.each_slice(size).map { |g| format_group(g) }.join("  ")
  end

  def self.format_printable(data)
    data.join.gsub(/[^[:print:]]/, ".")
  end

  def self.format_hex(data)
    data.chars.each_slice(LINE_SIZE).each_with_index do |bytes, i|
      yield format("%08i  %-#{GROUP_FORMAT_LENGTH}s  |%-#{LINE_SIZE}s|",
                   (i * LINE_SIZE), format_groups(bytes, GROUP_SIZE), format_printable(bytes))
    end

    nil
  end

  def self.puts(data, io: $stdout)
    format_hex(data) { |line| io.puts(line) }
  end
end
