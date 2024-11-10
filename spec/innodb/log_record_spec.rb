# frozen_string_literal: true

require "spec_helper"

describe Innodb::LogRecord do
  before :all do
    log_files = %w[
      spec/data/sakila/compact/ib_logfile0
      spec/data/sakila/compact/ib_logfile1
    ]
    @group = Innodb::LogGroup.new(log_files)
  end

  describe :INIT_FILE_PAGE do
    before(:all) do
      @rec = @group.reader.seek(8_204).record
    end
    it "has the correct size" do
      @rec.size.should eql 3
    end
    it "has the correct LSN" do
      @rec.lsn.should eql [8_204, 8_207]
    end
    it "has the correct preamble" do
      p = @rec.preamble
      p.type.should eql :INIT_FILE_PAGE
      p.page_number.should eql 1
      p.space.should eql 0
      p.single_record.should eql false
    end
    it "has an empty payload" do
      @rec.payload.should eql({})
    end
  end

  describe :IBUF_BITMAP_INIT do
    before(:all) do
      @rec = @group.reader.seek(8_207).record
    end
    it "has the correct size" do
      @rec.size.should eql 3
    end
    it "has the correct LSN" do
      @rec.lsn.should eql [8_207, 8_210]
    end
    it "has the correct preamble" do
      p = @rec.preamble
      p.type.should eql :IBUF_BITMAP_INIT
      p.page_number.should eql 1
      p.space.should eql 0
      p.single_record.should eql false
    end
    it "has an empty payload" do
      @rec.payload.should eql({})
    end
  end

  describe :REC_INSERT do
    before(:all) do
      @rec = @group.reader.seek(1_589_112).record
    end
    it "has the correct size" do
      @rec.size.should eql 36
    end
    it "has the correct LSN" do
      @rec.lsn.should eql [1_589_112, 1_589_148]
    end
    it "has the correct preamble" do
      p = @rec.preamble
      p.type.should eql :REC_INSERT
      p.page_number.should eql 9
      p.space.should eql 0
      p.single_record.should eql false
    end
    it "has the correct payload" do
      @rec.payload.values.first.should include(
        mismatch_index: 0,
        page_offset: 101,
        end_seg_len: 27,
        info_and_status_bits: 0,
        origin_offset: 8
      )
    end
  end
end
