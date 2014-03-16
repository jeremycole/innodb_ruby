# -*- encoding : utf-8 -*-
require 'spec_helper'

describe Innodb::LogRecord do
    before :all do
      log_files = ["spec/data/ib_logfile0", "spec/data/ib_logfile1"]
      @group = Innodb::LogGroup.new(log_files)
    end

    context :INIT_FILE_PAGE do
      before(:all) { @rec = @group.reader.seek(8204).record }
      subject { @rec }
      its(:size) { should == 3 }
      its(:lsn) { should =~ [8204,8207] }
      its(:preamble) do
        should eql(
          :type           => :INIT_FILE_PAGE,
          :page_number    => 1,
          :space          => 0,
          :single_record  => false)
      end
      its(:payload) { should eql({}) }
    end

    context :IBUF_BITMAP_INIT do
      before(:all) { @rec = @group.reader.seek(8207).record }
      subject { @rec }
      its(:size) { should == 3 }
      its(:lsn) { should =~ [8207,8210] }
      its(:preamble) do
        should eql(
          :type           => :IBUF_BITMAP_INIT,
          :page_number    => 1,
          :space          => 0,
          :single_record  => false)
      end
      its(:payload) { should eql({}) }
    end

    context :REC_INSERT do
      before(:all) { @rec = @group.reader.seek(1589112).record }
      subject { @rec }
      its(:size) { should == 36 }
      its(:lsn) { should =~ [1589112, 1589148] }
      its(:preamble) do
        should eql(
          :type           => :REC_INSERT,
          :page_number    => 9,
          :space          => 0,
          :single_record  => false)
      end
      its("payload.values.first") do
        should include(
          :mismatch_index       => 0,
          :page_offset          => 101,
          :end_seg_len          => 27,
          :info_and_status_bits => 0,
          :origin_offset        => 8)
      end
    end
end
