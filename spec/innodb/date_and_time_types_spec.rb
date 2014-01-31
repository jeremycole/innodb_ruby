# -*- encoding : utf-8 -*-

require 'spec_helper'

class Innodb::RecordDescriber::DateTimeTypes < Innodb::RecordDescriber
  type :clustered
  key "c01", "INT", :NOT_NULL
  row "c02", "YEAR"
  row "c03", "TIME"
  row "c04", "DATE"
  row "c05", "DATETIME"
  row "c06", "TIMESTAMP"
end

# Zero.
row0 = ["0000", "00:00:00", "0000-00-00", "0000-00-00 00:00:00", "0000-00-00 00:00:00"]

# Minimum values.
row1 = ["1901", "-838:59:59", "1000-01-01", "1000-01-01 00:00:00", "1970-01-01 00:00:01"]

# Maximum values.
row2 = ["2155", "838:59:59", "9999-12-31", "9999-12-31 23:59:59", "2038-01-19 03:14:07"]

# Random values.
row3 = ["2153", "20:47:10", "3275-11-07", "5172-01-24 13:36:22", "1985-03-16 18:35:56"]

describe Innodb::RecordDescriber do
  before :all do
    @space = Innodb::Space.new("spec/data/t_date_and_time_types.ibd")
    @space.record_describer = Innodb::RecordDescriber::DateTimeTypes.new
  end

  describe "#index" do
    it "is an Innodb::Index" do
      @space.index(3).should be_an_instance_of Innodb::Index
    end
  end

  describe "#each_record" do
    it "matches the expected values" do
      rec = @space.index(3).each_record
      rec.next.row.map { |f| f[:value] }.should =~ row0
      rec.next.row.map { |f| f[:value] }.should =~ row1
      rec.next.row.map { |f| f[:value] }.should =~ row2
      rec.next.row.map { |f| f[:value] }.should =~ row3
    end
  end
end
