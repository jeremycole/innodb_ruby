# -*- encoding : utf-8 -*-

# Collect stats globally within innodb_ruby for comparison purposes and for
# correctness checking.
class Innodb::Stats
  @@data = Hash.new(0)

  # Return the data hash directly.
  def self.data
    @@data
  end

  # Increment a statistic by name (typically a symbol), optionally by a value
  # provided.
  def self.increment(name, value=1)
    @@data[name] += value
  end

  # Get a statistic by name.
  def self.get(name)
    @@data[name]
  end

  # Reset all statistics.
  def self.reset
    @@data.clear
    nil
  end

  # Print a simple report of collected statistics, optionally to the IO object
  # provided, or by default to STDOUT.
  def self.print_report(io=STDOUT)
    io.puts "%-50s%10s" % [
      "Statistic",
      "Count",
    ]
    @@data.sort.each do |name, count|
      io.puts "%-50s%10i" % [
        name,
        count
      ]
    end

    nil
  end
end
