# frozen_string_literal: true

require "forwardable"

module Innodb
  class Record
    extend Forwardable

    attr_reader :page
    attr_accessor :record

    def initialize(page, record)
      @page = page
      @record = record
    end

    def_delegator :record, :header
    def_delegator :record, :offset
    def_delegator :record, :length
    def_delegator :record, :next
    def_delegator :record, :key
    def_delegator :record, :row
    def_delegator :record, :transaction_id
    def_delegator :record, :roll_pointer
    def_delegator :record, :child_page_number

    def_delegator :header, :type
    def_delegator :header, :heap_number
    def_delegator :header, :n_owned
    def_delegator :header, :heap_number
    def_delegator :header, :deleted?
    def_delegator :header, :min_rec?

    def key_string
      key&.map { |r| "%s=%s" % [r.name, r.value.inspect] }&.join(", ")
    end

    def row_string
      row&.map { |r| "%s=%s" % [r.name, r.value.inspect] }&.join(", ")
    end

    def full_value_with_externs_for_field(field)
      blob_value = field.value
      extern_page = field.extern && page.space.page(field.extern.page_number)
      while extern_page
        blob_value += extern_page.blob_data
        extern_page = extern_page.next_blob_page
      end
      blob_value
    end

    def undo
      return nil unless roll_pointer
      return unless (innodb_system = @page.space.innodb_system)

      undo_page = innodb_system.system_space.page(roll_pointer.undo_log.page)
      return unless undo_page

      new_undo_record = Innodb::UndoRecord.new(undo_page, roll_pointer.undo_log.offset)
      new_undo_record.index_page = page
      new_undo_record
    end

    def each_undo_record
      return enum_for(:each_undo_record) unless block_given?

      undo_record = undo
      while undo_record
        yield undo_record
        undo_record = undo_record.prev_by_history
      end

      nil
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
      %i[key row].each do |group|
        record[group]&.each do |column|
          fields_hash[column.name] = column.value
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
        return -1 if other_key[i] < key[i].value
        return +1 if other_key[i] > key[i].value
      end

      0
    end

    def dump
      puts "Record at offset %i" % offset
      puts

      puts "Header:"
      puts "  %-20s: %i" % ["Next record offset", header.next]
      puts "  %-20s: %i" % ["Heap number", header.heap_number]
      puts "  %-20s: %s" % ["Type", header.type]
      puts "  %-20s: %s" % ["Deleted", header.deleted?]
      puts "  %-20s: %s" % ["Length", header.length]
      puts

      if page.leaf?
        puts "System fields:"
        puts "  Transaction ID: %s" % transaction_id
        puts "  Roll Pointer:"
        puts "    Undo Log: page %i, offset %i" % [
          roll_pointer.undo_log.page,
          roll_pointer.undo_log.offset,
        ]
        puts "    Rollback Segment ID: %i" % roll_pointer.rseg_id
        puts "    Insert: %s" % roll_pointer.is_insert
        puts
      end

      puts "Key fields:"
      key.each do |field|
        puts "  %s: %s" % [
          field.name,
          field.value.inspect,
        ]
      end
      puts

      if page.leaf?
        puts "Non-key fields:"
        row.each do |field|
          puts "  %s: %s" % [
            field.name,
            field.value.inspect,
          ]
        end
      else
        puts "Child page number: %i" % child_page_number
      end
      puts
    end
  end
end
