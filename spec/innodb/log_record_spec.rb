# frozen_string_literal: true

require 'spec_helper'

describe Innodb::LogRecord do
  before :all do
    log_files = %w[
      spec/data/ib_logfile0
      spec/data/ib_logfile1
    ]
    @group = Innodb::LogGroup.new(log_files)
  end

  describe :INIT_FILE_PAGE do
    before(:all) do
      @rec = @group.reader.seek(8_204).record
    end
    it 'has the correct size' do
      @rec.size.should eql 3
    end
    it 'has the correct LSN' do
      @rec.lsn.should eql [8_204, 8_207]
    end
    it 'has the correct preamble' do
      @rec.preamble.should eql(
        type: :INIT_FILE_PAGE,
        page_number: 1,
        space: 0,
        single_record: false
      )
    end
    it 'has an empty payload' do
      @rec.payload.should eql({})
    end
  end

  describe :IBUF_BITMAP_INIT do
    before(:all) do
      @rec = @group.reader.seek(8_207).record
    end
    it 'has the correct size' do
      @rec.size.should eql 3
    end
    it 'has the correct LSN' do
      @rec.lsn.should eql [8_207, 8_210]
    end
    it 'has the correct preamble' do
      @rec.preamble.should eql(
        type: :IBUF_BITMAP_INIT,
        page_number: 1,
        space: 0,
        single_record: false
      )
    end
    it 'has an empty payload' do
      @rec.payload.should eql({})
    end
  end

  describe :REC_INSERT do
    before(:all) do
      @rec = @group.reader.seek(1_589_112).record
    end
    it 'has the correct size' do
      @rec.size.should eql 36
    end
    it 'has the correct LSN' do
      @rec.lsn.should eql [1_589_112, 1_589_148]
    end
    it 'has the correct preamble' do
      @rec.preamble.should eql(
        type: :REC_INSERT,
        page_number: 9,
        space: 0,
        single_record: false
      )
    end
    it 'has the correct payload' do
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
