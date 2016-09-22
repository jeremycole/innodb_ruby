# -*- encoding : utf-8 -*-
require 'spec_helper'

describe Innodb::LogRecord do
  before :all do
    log_files = ["spec/data/ib_logfile0", "spec/data/ib_logfile1"]
    @group = Innodb::LogGroup.new(log_files)
  end

  describe :INIT_FILE_PAGE do
    before(:all) do
      @rec = @group.reader.seek(8204).record
    end
    it "has the correct size" do
      @rec.size.should eql 3
    end
    it "has the correct LSN" do
      @rec.lsn.should eql [8204, 8207]
    end
    it "has the correct preamble" do
      @rec.preamble.should eql(
        :type           => :INIT_FILE_PAGE,
        :page_number    => 1,
        :space          => 0,
        :single_record  => false)
    end
    it "has an empty payload" do
      @rec.payload.should eql({})
    end
  end

  describe :IBUF_BITMAP_INIT do
    before(:all) do
      @rec = @group.reader.seek(8207).record
    end
    it "has the correct size" do
      @rec.size.should eql 3
    end
    it "has the correct LSN" do
      @rec.lsn.should eql [8207, 8210]
    end
    it "has the correct preamble" do
      @rec.preamble.should eql(
        :type           => :IBUF_BITMAP_INIT,
        :page_number    => 1,
        :space          => 0,
        :single_record  => false)
    end
    it "has an empty payload" do
      @rec.payload.should eql({})
    end
  end

  describe :REC_INSERT do
    before(:all) do
      @rec = @group.reader.seek(1589112).record
    end
    it "has the correct size" do
      @rec.size.should eql 36
    end
    it "has the correct LSN" do
      @rec.lsn.should eql [1589112, 1589148]
    end
    it "has the correct preamble" do
      @rec.preamble.should eql(
        :type           => :REC_INSERT,
        :page_number    => 9,
        :space          => 0,
        :single_record  => false)
    end
    it "has the correct payload" do
      @rec.payload.values.first.should include(
        :mismatch_index       => 0,
        :page_offset          => 101,
        :end_seg_len          => 27,
        :info_and_status_bits => 0,
        :origin_offset        => 8)
    end
  end
end
