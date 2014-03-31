# -*- encoding : utf-8 -*-

class Innodb::Record
  attr_reader :page
  attr_accessor :record

  def initialize(page, record)
    @page = page
    @record = record
  end

  def header
    record[:header]
  end

  def offset
    record[:offset]
  end

  def length
    record[:length]
  end

  def next
    record[:next]
  end

  def key
    record[:key]
  end

  def key_string
    key && key.map { |r| "%s=%s" % [r[:name], r[:value].inspect] }.join(", ")
  end

  def row
    record[:row]
  end

  def row_string
    row && row.map { |r| "%s=%s" % [r[:name], r[:value].inspect] }.join(", ")
  end

  def transaction_id
    record[:transaction_id]
  end

  def roll_pointer
    record[:roll_pointer]
  end

  def undo
    if innodb_system = @page.space.innodb_system
      undo_space = innodb_system.system_space
      if undo_page = undo_space.page(roll_pointer[:undo_log][:page])
        new_undo_record = Innodb::UndoRecord.new(undo_page, roll_pointer[:undo_log][:offset])
        new_undo_record.index_page = page
        new_undo_record
      end
    end
  end

  def each_undo_record
    unless block_given?
      return enum_for(:each_undo_record)
    end

    undo_record = undo
    while undo_record
      yield undo_record
      undo_record = undo_record.prev_by_history
    end

    nil
  end

  def child_page_number
    record[:child_page_number]
  end

  def string
    if child_page_number
      "(%s) → #%s" % [key_string, child_page_number]
    else
      "(%s) → (%s)" % [key_string, row_string]
    end
  end

  def uncached_fields
    fields_hash = {}
    [:key, :row].each do |group|
      if record[group]
        record[group].each do |column|
          fields_hash[column[:name]] = column[:value]
        end
      end
    end
    fields_hash
  end

  def fields
    @fields ||= uncached_fields
  end

  # Compare two arrays of fields to determine if they are equal. This follows
  # the same comparison rules as strcmp and others:
  #   0 = a is equal to b
  #   -1 = a is less than b
  #   +1 = a is greater than b
  def compare_key(other_key)
    Innodb::Stats.increment :compare_key

    return 0 if other_key.nil? && key.nil?
    return -1 if other_key.nil? || (!key.nil? && other_key.size < key.size)
    return +1 if key.nil? || (!other_key.nil? && other_key.size > key.size)

    key.each_index do |i|
      Innodb::Stats.increment :compare_key_field_comparison
      return -1 if other_key[i] < key[i][:value]
      return +1 if other_key[i] > key[i][:value]
    end

    return 0
  end
end
