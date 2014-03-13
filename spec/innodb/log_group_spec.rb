# -*- encoding : utf-8 -*-
require 'spec_helper'

LOG_FILES = ["spec/data/ib_logfile0", "spec/data/ib_logfile1"]

describe Innodb::LogGroup do
  before :all do
    @log_group = Innodb::LogGroup.new(LOG_FILES)
  end

  describe "#new" do
    it "returns an Innodb::LogGroup" do
      @log_group.should be_a Innodb::LogGroup
    end
  end

  describe "each_log" do
    subject { @log_group.each_log }

    it "is an enumerator" do
      is_enumerator?(subject).should be_true
    end

    it "yields an Innodb::Log" do
      2.times { subject.next.should be_a Innodb::Log }
      expect { subject.next }.to raise_error(StopIteration)
    end
  end

  describe "each_block" do
    subject { @log_group.each_block }

    it "is an enumerator" do
      is_enumerator?(subject).should be_true
    end

    it "yields an Innodb::LogBlock" do
      subject.next.last.should be_a Innodb::LogBlock
    end
  end
end
