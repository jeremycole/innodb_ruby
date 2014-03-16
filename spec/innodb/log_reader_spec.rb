# -*- encoding : utf-8 -*-
require 'spec_helper'

describe Innodb::LogReader do
  before :all do
    log_files = ["spec/data/ib_logfile0", "spec/data/ib_logfile1"]
    @group = Innodb::LogGroup.new(log_files)
    @reader = @group.reader
  end

  describe "#seek" do
    it "repositions the reader" do
      @reader.seek(8204).tell.should eql 8204
    end

    it "detects out of bounds seeks" do
      expect { @reader.seek(8192) }.to raise_error "LSN 8192 is out of bounds"
    end
  end

  describe "#tell" do
    it "returns the current LSN position" do
      @reader.seek(8205).tell.should eql 8205
      @reader.seek(8204).tell.should eql 8204
    end
  end

  describe "#record" do
    it "returns an instance of Innodb::LogRecord"  do
      record = @reader.record
      record.should be_an_instance_of Innodb::LogRecord
    end

    it "repositions the reader after reading a record" do
      @reader.tell.should eql 8207
    end

    it "reads records across blocks" do
      512.times { @reader.record.should_not be_nil }
    end
  end
end
