# frozen_string_literal: true

require 'forwardable'

# A single undo log record.
module Innodb
  class UndoRecord
    extend Forwardable

    Header = Struct.new(
      :prev,
      :next,
      :type,
      :extern_flag,
      :info,
      keyword_init: true
    )

    HeaderInfo = Struct.new(
      :order_may_change,
      :size_may_change,
      keyword_init: true
    )

    Record = Struct.new(
      :page,
      :offset,
      :header,
      :undo_no,
      :table_id,
      :info_bits,
      :trx_id,
      :roll_ptr,
      :data,
      :key,
      :row,
      keyword_init: true
    )

    Field = Struct.new(
      :name,
      :type,
      :value,
      keyword_init: true
    )

    attr_reader :undo_page
    attr_reader :position

    attr_accessor :undo_log
    attr_accessor :index_page

    def initialize(undo_page, position)
      @undo_page = undo_page
      @position = position

      @undo_log = nil
      @index_page = nil
    end

    def new_subordinate(undo_page, position)
      new_undo_record = self.class.new(undo_page, position)
      new_undo_record.undo_log = undo_log
      new_undo_record.index_page = index_page

      new_undo_record
    end

    # The header really starts 2 bytes before the undo record position, as the
    # pointer to the previous record is written there.
    def pos_header
      @position - 2
    end

    # The size of the header.
    def size_header
      2 + 2 + 1
    end

    def pos_record
      pos_header + size_header
    end

    # Return a BufferCursor starting before the header.
    def cursor(position)
      new_cursor = @undo_page.cursor(position)
      new_cursor.push_name("undo_log[#{@undo_log.position}]") if @undo_log
      new_cursor.push_name("undo_record[#{@position}]")
      new_cursor
    end

    # Possible undo record types.
    TYPE = {
      11 => :insert,
      12 => :update_existing,
      13 => :update_deleted,
      14 => :delete,
    }.freeze

    TYPES_WITH_PREVIOUS_VERSIONS = %i[
      update_existing
      update_deleted
      delete
    ].freeze

    TYPE_MASK = 0x0f
    COMPILATION_INFO_MASK = 0x70
    COMPILATION_INFO_SHIFT = 4
    COMPILATION_INFO_NO_ORDER_CHANGE_BV = 1
    COMPILATION_INFO_NO_SIZE_CHANGE_BV = 2
    EXTERN_FLAG = 0x80

    def header
      @header ||= cursor(pos_header).name('header') do |c|
        header = Header.new(
          prev: c.name('prev') { c.get_uint16 },
          next: c.name('next') { c.get_uint16 }
        )

        info = c.name('info') { c.get_uint8 }
        cmpl = (info & COMPILATION_INFO_MASK) >> COMPILATION_INFO_SHIFT
        header.type = TYPE[info & TYPE_MASK]
        header.extern_flag = (info & EXTERN_FLAG) != 0
        header.info = HeaderInfo.new(
          order_may_change: (cmpl & COMPILATION_INFO_NO_ORDER_CHANGE_BV).zero?,
          size_may_change: (cmpl & COMPILATION_INFO_NO_SIZE_CHANGE_BV).zero?
        )

        header
      end
    end

    def_delegator :header, :type

    def previous_version?
      TYPES_WITH_PREVIOUS_VERSIONS.include?(type)
    end

    def get(prev_or_next)
      return if header[prev_or_next].zero?

      new_undo_record = new_subordinate(@undo_page, header[prev_or_next])
      new_undo_record if new_undo_record.type
    end

    def prev
      get(:prev)
    end

    def next
      get(:next)
    end

    def record_size
      header[:next] - @position - size_header
    end

    def read_record
      cursor(pos_record).name('record') do |c|
        this_record = Record.new(
          page: undo_page.offset,
          offset: position,
          header: header,
          undo_no: c.name('undo_no') { c.get_imc_uint64 },
          table_id: c.name('table_id') { c.get_imc_uint64 }
        )

        if previous_version?
          this_record.info_bits = c.name('info_bits') { c.get_uint8 }
          this_record.trx_id = c.name('trx_id') { c.get_ic_uint64 }
          this_record.roll_ptr = c.name('roll_ptr') do
            Innodb::DataType::RollPointerType.parse_roll_pointer(c.get_ic_uint64)
          end
        end

        if index_page
          read_record_fields(this_record, c)
        else
          # Slurp up the remaining data as a string.
          this_record.data = c.get_bytes(header[:next] - c.position - 2)
        end

        this_record
      end
    end

    def read_record_fields(this_record, cursor)
      this_record.key = []
      index_page.record_format[:key].each do |field|
        length = cursor.name('field_length') { cursor.get_ic_uint32 }
        value = cursor.name(field.name) { field.value_by_length(cursor, length) }

        this_record.key[field.position] = Field.new(name: field.name, type: field.data_type.name, value: value)
      end

      return unless previous_version?

      field_count = cursor.name('field_count') { cursor.get_ic_uint32 }
      this_record.row = Array.new(index_page.record_format[:row].size)
      field_count.times do
        field_number = cursor.name("field_number[#{field_count}]") { cursor.get_ic_uint32 }
        field = nil
        field_index = nil
        index_page.record_format[:row].each_with_index do |candidate_field, index|
          if candidate_field.position == field_number
            field = candidate_field
            field_index = index
          end
        end

        raise "Unknown field #{field_number}" unless field

        length = cursor.name('field_length') { cursor.get_ic_uint32 }
        value = cursor.name(field.name) { field.value_by_length(cursor, length) }

        this_record.row[field_index] = Field.new(name: field.name, type: field.data_type.name, value: value)
      end
    end

    def undo_record
      @undo_record ||= read_record
    end

    def_delegator :undo_record, :undo_no
    def_delegator :undo_record, :table_id
    def_delegator :undo_record, :trx_id
    def_delegator :undo_record, :roll_ptr
    def_delegator :undo_record, :key
    def_delegator :undo_record, :page
    def_delegator :undo_record, :offset

    def key_string
      key&.map { |r| '%s=%s' % [r[:name], r[:value].inspect] }&.join(', ')
    end

    def row
      undo_record[:row]
    end

    def row_string
      row&.reject(&:nil?)&.map { |r| r && '%s=%s' % [r[:name], r[:value].inspect] }&.join(', ')
    end

    def string
      '(%s) → (%s)' % [key_string, row_string]
    end

    # Find the previous row version by following the roll_ptr from one undo
    # record to the next (backwards through the record version history). Since
    # we are operating without the benefit of knowing about active transactions
    # and without protection from purge, check that everything looks sane before
    # returning it.
    def prev_by_history
      # This undo record type has no previous version information.
      return unless previous_version?

      undo_log = roll_ptr[:undo_log]
      older_undo_page = @undo_page.space.page(undo_log[:page])

      # The page was probably re-used for something else.
      return unless older_undo_page&.is_a?(Innodb::Page::UndoLog)

      older_undo_record = new_subordinate(older_undo_page, undo_log[:offset])

      # The record space was probably re-used for something else.
      return unless older_undo_record && table_id == older_undo_record.table_id

      # The trx_id should not be newer; but may be absent (for insert).
      return unless older_undo_record.trx_id.nil? || trx_id >= older_undo_record.trx_id

      older_undo_record
    end

    def dump
      puts 'Undo record at offset %i' % offset
      puts

      puts 'Header:'
      puts '  %-25s: %i' % ['Previous record offset', header[:prev]]
      puts '  %-25s: %i' % ['Next record offset', header[:next]]
      puts '  %-25s: %s' % ['Type', header[:type]]
      puts

      puts 'System fields:'
      puts '  Transaction ID: %s' % trx_id
      puts '  Roll Pointer:'
      puts '    Undo Log: page %i, offset %i' % [
        roll_ptr[:undo_log][:page],
        roll_ptr[:undo_log][:offset],
      ]
      puts '    Rollback Segment ID: %i' % roll_ptr[:rseg_id]
      puts

      puts 'Key fields:'
      key.each do |field|
        puts '  %s: %s' % [
          field[:name],
          field[:value].inspect,
        ]
      end
      puts

      puts 'Non-key fields:'
      row.each do |field|
        next unless field

        puts '  %s: %s' % [
          field[:name],
          field[:value].inspect,
        ]
      end
      puts
    end
  end
end
