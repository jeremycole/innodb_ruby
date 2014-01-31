# -*- encoding : utf-8 -*-

class Innodb::LogGroup
  def initialize
    @files = []
  end

  def add_log(file)
    if file = Innodb::Log.new(file)
      @files.push file
    else
      raise "Couldn't open #{file}"
    end
  end

  def each_block
    @files.each do |file|
      file.each_block do |block_number, block|
        yield block_number, block
      end
    end
  end

  def current_tail_position
    max       = 0
    max_file  = nil
    max_block = nil

    @files.each_with_index do |file, file_number|
      file.each_block do |block_number, block|
        if block.header[:block] > max
          max   = block.header[:block]
          max_file  = file_number
          max_block = block_number
        end
      end
    end

    { :file => max_file, :block => max_block }
  end

  def successor_position(position)
    if position[:block] == @files[position[:file]].blocks
      if position[:file] == @files.size
        { :file => 0, :block => 0 }
      else
        { :file => position[:file] + 1, :block => 0 }
      end
    else
      { :file => position[:file], :block => position[:block] + 1 }
    end
  end

  def block(file_number, block_number)
    @files[file_number].block(block_number)
  end

  def block_if_newer(old_block, new_block)
    return new_block if old_block.nil?
    #puts "old: #{old_block.header[:block]} new: #{new_block.header[:block]}"
    if new_block.header[:block] >= old_block.header[:block]
      new_block
    end
  end

  def tail_blocks
    position = current_tail_position
    current_block = nil
    while true
      until block_if_newer(current_block, new_block = block(position[:file], position[:block]))
        #puts "Waiting at the tail: #{position[:file]} #{position[:block]}"
        sleep 0.1
      end
      yield new_block
      position = successor_position(position)
      current_block = new_block
    end
  end
end
