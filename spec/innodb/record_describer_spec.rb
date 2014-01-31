# -*- encoding : utf-8 -*-

require 'spec_helper'

class Innodb::RecordDescriber::Test < Innodb::RecordDescriber
  type :clustered
  key "id",     :BIGINT, :UNSIGNED, :NOT_NULL
  row "a",      "INT"
  row "b",      "VARCHAR(64)"
  row "c",      "INT", :NOT_NULL
  row "d",      "VARCHAR(128)", :NOT_NULL
  row "e",      "MEDIUMINT", :UNSIGNED
  row "f",      "VARBINARY(512)"
  row "g",      "BIGINT", :UNSIGNED
  row "h",      "BLOB"
end

describe Innodb::RecordDescriber do
  before :all do
    @space = Innodb::Space.new("spec/data/t_record_describer.ibd")
    @space.record_describer = Innodb::RecordDescriber::Test.new
  end

  describe "#index" do
    it "is an Innodb::Index" do
      @space.index(3).should be_an_instance_of Innodb::Index
    end
  end

  describe "#each_record" do
    it "iterates through records" do
      @space.index(3).each_record.to_a.size.should eql 2
    end
  end

  describe "first record" do
    it "has one NULL field" do
      rec = @space.index(3).each_record.next
      rec.header[:field_nulls].count(true).should eql 1
    end

    it "has one externally stored field" do
      rec = @space.index(3).each_record.next
      rec.header[:field_externs].count(true).should eql 1
    end

    it "key is (1)" do
      rec = @space.index(3).each_record.next
      fields = rec.key.each
      fields.next[:value].should eql 1
    end

    it "row is (-1, '1' * 64, 1, '1' * 128, 1, NULL, 1, '1' * 16384)" do
      rec = @space.index(3).each_record.next
      fields = rec.row.each
      fields.next[:value].should eql -1
      fields.next[:value].should eql '1' * 64
      fields.next[:value].should eql 1
      fields.next[:value].should eql '1' * 128
      fields.next[:value].should eql 1
      fields.next[:value].should eql :NULL
      fields.next[:value].should eql 1
      fields.next[:value].should eql '1' * 768 # max prefix
    end

    it "external reference field is [4, 4, 38, 15616]" do
      rec = @space.index(3).each_record.next
      extern = rec.row.last[:extern]
      extern[:space_id].should eql 4
      extern[:page_number].should eql 4
      extern[:offset].should eql 38
      extern[:length].should eql 15616 # 16384 - 768
    end
  end
end
