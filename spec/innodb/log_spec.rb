# -*- encoding : utf-8 -*-
require 'spec_helper'

describe Innodb::Log do
  LOG_CHECKPOINT_FSP_MAGIC_N_VAL = 1441231243

  before :all do
    @log = Innodb::Log.new("spec/data/ib_logfile0")
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
    it "returns 5242880 bytes" do
      @log.size.should eql 5242880
    end
  end

  describe "#blocks" do
    it "returns 10236 blocks" do
      @log.blocks.should eql 10236
    end
  end

  describe "#block" do
    it "returns an Innodb::Block" do
      @log.block(0).should be_an_instance_of Innodb::LogBlock
    end

    it "does not return an invalid block" do
      @log.block(-1).should be_nil
      @log.block(10236).should be_nil
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
    it "returns a Hash" do
      @log.header.should be_an_instance_of Hash
    end

    it "has only Symbol keys" do
      classes = @log.header.keys.map { |k| k.class }.uniq
      classes.should eql [Symbol]
    end

    it "has the right keys and values" do
      @log.header.size.should eql 4
      @log.header.should include(
        :group_id   => 0,
        :start_lsn  => 8192,
        :file_no    => 0,
        :created_by => "    ")
    end
  end

  describe "#checkpoint" do
    it "returns a Hash" do
      @log.checkpoint.should be_an_instance_of Hash
      @log.checkpoint.size.should eql 2
    end

    it "has only Symbol keys" do
      classes = @log.checkpoint.keys.map { |k| k.class }.uniq
      classes.should eql [Symbol]
    end

    it "has a correct checkpoint_1" do
      @log.checkpoint[:checkpoint_1].should include(
        :number         => 10,
        :lsn            => 1603732,
        :lsn_offset     => 1597588,
        :buffer_size    => 1048576,
        :archived_lsn   => 18446744073709551615,
        # :group_array
        :checksum_1     => 654771786,
        :checksum_2     => 1113429956,
        :fsp_free_limit => 5,
        :fsp_magic      => LOG_CHECKPOINT_FSP_MAGIC_N_VAL)
    end

    it "has a correct checkpoint_2" do
      @log.checkpoint[:checkpoint_2].should include(
        :number         => 11,
        :lsn            => 1603732,
        :lsn_offset     => 1597588,
        :buffer_size    => 1048576,
        :archived_lsn   => 18446744073709551615,
        # :group_array
        :checksum_1     => 843938123,
        :checksum_2     => 674570893,
        :fsp_free_limit => 5,
        :fsp_magic      => LOG_CHECKPOINT_FSP_MAGIC_N_VAL)
    end
  end
end
