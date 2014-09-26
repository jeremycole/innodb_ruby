# -*- encoding : utf-8 -*-

require 'spec_helper'

class PkRecordDescriber < Innodb::RecordDescriber
  type :clustered
  key "c1", :BIGINT,          :NOT_NULL, :UNSIGNED
  row "c2", "INT"
  row "c3", "VARCHAR(64)"
  key "c4", "INT",            :NOT_NULL
  row "c5", "VARCHAR(128)",   :NOT_NULL
  row "c6", "MEDIUMINT",      :UNSIGNED
  row "c7", "VARBINARY(512)"
  row "c8", "BIGINT",         :UNSIGNED
  row "c9", "BLOB"
end

class SkRecordDescriber < Innodb::RecordDescriber
  type :secondary
  row "c1", :BIGINT,          :NOT_NULL, :UNSIGNED
  row "c4", "INT",            :NOT_NULL
  key "c6", "MEDIUMINT",      :UNSIGNED
  key "c8", "BIGINT",         :UNSIGNED
end

describe Innodb::RecordDescriber do
  context "PkRecordDescriber" do
    before :all do
      @space = Innodb::Space.new("spec/data/t_record_describer.ibd")
      @space.record_describer = PkRecordDescriber.new
      @index = @space.index(3)
    end

    describe "#index" do
      it "is an Innodb::Index" do
        @index.should be_an_instance_of Innodb::Index
      end
    end

    describe "#each_record" do
      it "iterates through records" do
        @index.each_record.to_a.size.should eql 210
      end
    end

    context "#min_record" do
      before :all do
        @rec = @index.min_record
      end

      it "has one NULL field" do
        @rec.header[:nulls].size.should eql 1
      end

      it "has one externally stored field" do
        @rec.header[:externs].size.should eql 1
      end

      it "#transaction_id" do
        @rec.transaction_id.should eql 2305
      end

      it "#roll_pointer" do
        @rec.roll_pointer.should eql(
          :is_insert => true,
          :undo_log  => {
            :offset  => 272,
            :page    => 435},
          :rseg_id => 2)
      end

      it "key is (1, 1)" do
        fields = @rec.key.each
        fields.next[:value].should eql 1
        fields.next[:value].should eql 1
      end

      it "row is (-1, '1' * 64, '1' * 128, 1, NULL, 1, '1' * 16384)" do
        fields = @rec.row.each
        fields.next[:value].should eql -1
        fields.next[:value].should eql '1' * 64
        fields.next[:value].should eql '1' * 128
        fields.next[:value].should eql 1
        fields.next[:value].should eql :NULL
        fields.next[:value].should eql 1
        fields.next[:value].should eql '1' * 768 # max prefix
      end

      it "external reference field is [6, 5, 38, 15616]" do
        extern = @rec.row.last[:extern]
        extern[:space_id].should eql 6
        extern[:page_number].should eql 5
        extern[:offset].should eql 38
        extern[:length].should eql 15616 # 16384 - 768
      end
    end

    context "#root.min_record" do
      before :all do
        @rec = @index.root.min_record
      end

      it "#header" do
        @rec.header.should include(
          :type         => :node_pointer,
          :length       => 5,
          :min_rec      => true,
          :heap_number  => 2,
          :deleted      => false)
        @rec.header[:nulls].size.should eql 0
        @rec.header[:lengths].size.should eql 0
      end

      it "#child_page_number" do
        @rec.child_page_number.should eql 10
      end

      it "#key" do
        @rec.key.size.should eql 2
        @rec.fields["c1"].should eql 1
        @rec.fields["c4"].should eql 1
      end
    end
  end

  context "SkRecordDescriber" do
    before :all do
      @space = Innodb::Space.new("spec/data/t_record_describer.ibd")
      @space.record_describer = SkRecordDescriber.new
      @index = @space.index(4)
    end

    describe "#index" do
      it "is an Innodb::Index" do
        @index.should be_an_instance_of Innodb::Index
      end
    end

    describe "#each_record" do
      it "iterates through records" do
        @index.each_record.to_a.size.should eql 210
      end
    end

    context "#min_record" do
      before :all do
        @rec = @index.min_record
      end

      it "key is (1, 1)" do
        rec = @index.each_record.next
        fields = rec.key.each
        fields.next[:value].should eql 1
        fields.next[:value].should eql 1
      end

      it "row is (1, 1)" do
        rec = @index.each_record.next
        fields = rec.row.each
        fields.next[:value].should eql 1
        fields.next[:value].should eql 1
      end
    end
  end
end
