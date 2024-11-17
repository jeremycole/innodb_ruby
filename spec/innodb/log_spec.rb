# frozen_string_literal: true

require "spec_helper"

describe Innodb::Log do
  before :all do
    @log = Innodb::Log.new("spec/data/sakila/compact/ib_logfile0")
  end

  describe "#new" do
    it "defines a class" do
      Innodb::Log.should be_an_instance_of Class
    end

    it "returns an Innodb::Log" do
      @log.should be_an_instance_of Innodb::Log
    end
  end

  describe "#size" do
    it "returns 5,242,880 bytes" do
      @log.size.should eql 5_242_880
    end
  end

  describe "#blocks" do
    it "returns 10,236 blocks" do
      @log.blocks.should eql 10_236
    end
  end

  describe "#block" do
    it "returns an Innodb::Block" do
      @log.block(0).should be_an_instance_of Innodb::LogBlock
    end

    it "does not return an invalid block" do
      @log.block(-1).should be_nil
      @log.block(10_236).should be_nil
    end
  end

  describe "#block_data" do
    it "returns block data at offset" do
      @log.block_data(0).should_not be_nil
      expect { @log.block_data(256) }.to raise_error "Invalid block offset"
      expect { @log.block_data(513) }.to raise_error "Invalid block offset"
    end
  end

  describe "#header" do
    it "returns a Innodb::Log::Header" do
      @log.header.should be_an_instance_of Innodb::Log::Header
    end

    it "has the right keys and values" do
      @log.header.size.should eql 4
      @log.header.group_id.should eql 0
      @log.header.start_lsn.should eql 8_192
      @log.header.file_no.should eql 0
      @log.header.created_by.should eql "    "
    end
  end

  describe "#checkpoint" do
    it "returns a Innodb::Log::CheckpointSet" do
      @log.checkpoint.should be_an_instance_of Innodb::Log::CheckpointSet
      @log.checkpoint.size.should eql 2
    end

    it "has a correct checkpoint_1" do
      c = @log.checkpoint.checkpoint_1
      c.number.should eql 14
      c.lsn.should eql 8_400_260
      c.lsn_offset.should eql 8_396_164
      c.buffer_size.should eql 1_048_576
      c.archived_lsn.should eql 18_446_744_073_709_551_615
      c.checksum_1.should eql 2_424_759_900
      c.checksum_2.should eql 3_026_016_186
      c.fsp_free_limit.should eql 9
      c.fsp_magic.should eql 1_441_231_243 # LOG_CHECKPOINT_FSP_MAGIC_N_VAL
    end

    it "has a correct checkpoint_2" do
      c = @log.checkpoint.checkpoint_2
      c.number.should eql 15
      c.lsn.should eql 8_400_260
      c.lsn_offset.should eql 8_396_164
      c.buffer_size.should eql 1_048_576
      c.archived_lsn.should eql 18_446_744_073_709_551_615
      c.checksum_1.should eql 125_194_589
      c.checksum_2.should eql 1_139_538_825
      c.fsp_free_limit.should eql 9
      c.fsp_magic.should eql 1_441_231_243 # LOG_CHECKPOINT_FSP_MAGIC_N_VAL
    end
  end
end
