# -*- encoding : utf-8 -*-

require 'spec_helper'

class Innodb::RecordDescriber::NumericTypes < Innodb::RecordDescriber
  type :clustered
  key "c01", "INT",       :UNSIGNED, :NOT_NULL
  row "c02", "TINYINT"
  row "c03", "TINYINT",   :UNSIGNED
  row "c04", "SMALLINT"
  row "c05", "SMALLINT",  :UNSIGNED
  row "c06", "MEDIUMINT"
  row "c07", "MEDIUMINT", :UNSIGNED
  row "c08", "INT"
  row "c09", "INT",       :UNSIGNED
  row "c10", "BIGINT"
  row "c11", "BIGINT",    :UNSIGNED
  row "c12", "FLOAT"
  row "c13", "FLOAT"
  row "c14", "DOUBLE"
  row "c15", "DOUBLE"
  row "c16", "DECIMAL(10,0)"
  row "c17", "DECIMAL(10,0)", :UNSIGNED
  row "c18", "DECIMAL(65,0)"
  row "c19", "DECIMAL(35,30)"
  row "c20", "BIT"
  row "c21", "BIT(32)"
  row "c22", "BIT(64)"
end

# Zero.
row0 = [
 0,
 0,
 0,
 0,
 0,
 0,
 0,
 0,
 0,
 0,
 0.0,
 0.0,
 0.0,
 0.0,
 "0.0",
 "0.0",
 "0.0",
 "0.0",
 "0b0",
 "0b0",
 "0b0"]

# Minus one.
row1 = [
 -1,
 0,
 -1,
 0,
 -1,
 0,
 -1,
 0,
 -1,
 0,
 -1.0,
 0.0,
 -1.0,
 0.0,
 "-1.0",
 "0.0",
 "-1.0",
 "-1.0",
 "0b1",
 "0b11111111111111111111111111111111",
 "0b1111111111111111111111111111111111111111111111111111111111111111"]

# One.
row2 = [
 1,
 1,
 1,
 1,
 1,
 1,
 1,
 1,
 1,
 1,
 1.0,
 1.0,
 1.0,
 1.0,
 "1.0",
 "1.0",
 "1.0",
 "1.0",
 "0b1",
 "0b1",
 "0b1"]

# Minimum values.
row3 = [
 -128,
 0,
 -32768,
 0,
 -8388608,
 0,
 -2147483648,
 0,
 -9223372036854775808,
 0,
 -1.1754943508222875e-38,
 0.0,
 -2.2250738585072014e-208,
 0.0,
 "-9999999999.0",
 "0.0",
 "-99999999999999999999999999999999999999999999999999999999999999999.0",
 "-99999.999999999999999999999999999999",
 "0b0",
 "0b0",
 "0b0"]

# Maximum values.
row4 = [
 127,
 255,
 32767,
 65535,
 8388607,
 16777215,
 2147483647,
 4294967295,
 9223372036854775807,
 18446744073709551615,
 3.4028234663852886e+38,
 3.4028234663852886e+38,
 1.7976931348623157e+308,
 1.7976931348623157e+308,
 "9999999999.0",
 "9999999999.0",
 "99999999999999999999999999999999999999999999999999999999999999999.0",
 "99999.999999999999999999999999999999",
 "0b1",
 "0b11111111111111111111111111111111",
 "0b1111111111111111111111111111111111111111111111111111111111111111"]

# Random values.
row5 = [
 -92,
 216,
 -21244,
 37375,
 -2029076,
 13161062,
 -561256167,
 2859565307,
 -2989164089322500559,
 4909805763357741578,
 8.007314291015967e+37,
 2.3826953364781035e+38,
 -1.0024988592301854e+308,
 3.8077578553713446e+307,
 "-2118290683.0",
 "7554694345.0",
 "36896958284301606307227443682014665342058559023876912710455539626.0",
 "59908.987290718443144993967601373349",
 "0b0",
 "0b1110000001101111100011001110100",
 "0b1001001010001001000111001010000011000011110110011100000101000010"]

describe Innodb::RecordDescriber do
  before :all do
    @space = Innodb::Space.new("spec/data/t_numeric_types.ibd")
    @space.record_describer = Innodb::RecordDescriber::NumericTypes.new
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
      rec.next.row.map { |f| f[:value] }.should =~ row4
      rec.next.row.map { |f| f[:value] }.should =~ row5
    end
  end
end
