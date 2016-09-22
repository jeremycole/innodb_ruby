# -*- encoding : utf-8 -*-
require 'spec_helper'

describe Innodb::LogGroup do
  before :all do
    @log_files = ["spec/data/ib_logfile0", "spec/data/ib_logfile1"]
    @log_file_size = 5242880
    @log_group = Innodb::LogGroup.new(@log_files)
  end

  describe "#new" do
    it "returns an Innodb::LogGroup" do
      @log_group.should be_a Innodb::LogGroup
    end
  end

  describe "#each_log" do
    subject { @log_group.each_log }

    it "is an enumerator" do
      is_enumerator?(subject).should be_truthy
    end

    it "returns an Innodb::Log" do
      @log_files.size.times { subject.next.should be_a Innodb::Log }
      expect { subject.next }.to raise_error(StopIteration)
    end
  end

  describe "#each_block" do
    subject { @log_group.each_block }

    it "is an enumerator" do
      is_enumerator?(subject).should be_truthy
    end

    it "returns an Innodb::LogBlock" do
      subject.next.last.should be_a Innodb::LogBlock
    end
  end

  describe "#logs" do
    it "returns the number of logs" do
      @log_group.logs.should eql @log_files.size
    end
  end

  describe "#log_size" do
    it "returns the log file size" do
      @log_group.log_size.should eql @log_file_size
    end
  end

  describe "#size" do
    it "returns the log group size" do
      @log_group.size.should eql @log_file_size * @log_files.size
    end
  end

  describe "#capacity" do
    it "returns the group capacity size" do
      capacity = (@log_file_size - Innodb::Log::LOG_HEADER_SIZE) * @log_files.size
      @log_group.capacity.should eql capacity
    end
  end

  describe "#reader" do
    it "returns an instance of Innodb::LogReader" do
      @log_group.reader.should be_a Innodb::LogReader
    end
  end

  describe "#record" do
    it "returns an instance of Innodb::LogRecord" do
      @log_group.record(8204).should be_a Innodb::LogRecord
    end
  end
end
