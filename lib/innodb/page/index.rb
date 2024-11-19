# frozen_string_literal: true

require "forwardable"

require "innodb/fseg_entry"

# A specialized class for handling INDEX pages, which contain a portion of
# the data from exactly one B+tree. These are typically the most common type
# of page in any database.
#
# The basic structure of an INDEX page is: FIL header, INDEX header, FSEG
# header, fixed-width system records (infimum and supremum), user records
# (the actual data) which grow ascending by offset, free space, the page
# directory which grows descending by offset, and the FIL trailer.
module Innodb
  class Page
    class Index < Page
      extend Forwardable

      specialization_for :INDEX

      RecordHeader = Struct.new(
        :length, # rubocop:disable Lint/StructNewOverride
        :next,
        :type,
        :heap_number,
        :n_owned,
        :info_flags,
        :offset_size,
        :n_fields,
        :nulls,
        :lengths,
        :externs,
        keyword_init: true
      )

      class RecordHeader
        # This record is the minimum record at this level of the B-tree.
        RECORD_INFO_MIN_REC_FLAG = 1

        # This record has been marked as deleted.
        RECORD_INFO_DELETED_FLAG = 2

        def min_rec?
          (info_flags & RECORD_INFO_MIN_REC_FLAG) != 0
        end

        def deleted?
          (info_flags & RECORD_INFO_DELETED_FLAG) != 0
        end
      end

      SystemRecord = Struct.new(
        :offset,
        :header,
        :next,
        :data,
        :length, # rubocop:disable Lint/StructNewOverride
        keyword_init: true
      )

      UserRecord = Struct.new(
        :type,
        :format,
        :offset,
        :header,
        :next,
        :key,
        :row,
        :sys,
        :child_page_number,
        :transaction_id,
        :roll_pointer,
        :length, # rubocop:disable Lint/StructNewOverride
        keyword_init: true
      )

      FieldDescriptor = Struct.new(
        :name,
        :type,
        :value,
        :extern,
        keyword_init: true
      )

      FsegHeader = Struct.new(
        :leaf,
        :internal,
        keyword_init: true
      )

      PageHeader = Struct.new(
        :n_dir_slots,
        :heap_top,
        :n_heap_format,
        :n_heap,
        :format,
        :garbage_offset,
        :garbage_size,
        :last_insert_offset,
        :direction,
        :n_direction,
        :n_recs,
        :max_trx_id,
        :level,
        :index_id,
        keyword_init: true
      )

      # The size (in bytes) of the "next" pointer in each record header.
      RECORD_NEXT_SIZE = 2

      # The size (in bytes) of the bit-packed fields in each record header for
      # "redundant" record format.
      RECORD_REDUNDANT_BITS_SIZE = 4

      # Masks for 1-byte record end-offsets within "redundant" records.
      RECORD_REDUNDANT_OFF1_OFFSET_MASK   = 0x7f
      RECORD_REDUNDANT_OFF1_NULL_MASK     = 0x80

      # Masks for 2-byte record end-offsets within "redundant" records.
      RECORD_REDUNDANT_OFF2_OFFSET_MASK   = 0x3fff
      RECORD_REDUNDANT_OFF2_NULL_MASK     = 0x8000
      RECORD_REDUNDANT_OFF2_EXTERN_MASK   = 0x4000

      # The size (in bytes) of the bit-packed fields in each record header for
      # "compact" record format.
      RECORD_COMPACT_BITS_SIZE = 3

      # Maximum number of fields.
      RECORD_MAX_N_SYSTEM_FIELDS  = 3
      RECORD_MAX_N_FIELDS         = 1024 - 1
      RECORD_MAX_N_USER_FIELDS    = RECORD_MAX_N_FIELDS - (RECORD_MAX_N_SYSTEM_FIELDS * 2)

      # Page direction values possible in the page_header's :direction field.
      PAGE_DIRECTION = {
        1 => :left,           # Inserts have been in descending order.
        2 => :right,          # Inserts have been in ascending order.
        3 => :same_rec,       # Unused by InnoDB.
        4 => :same_page,      # Unused by InnoDB.
        5 => :no_direction,   # Inserts have been in random order.
      }.freeze

      # Record types used in the :type field of the record header.
      RECORD_TYPES = {
        0 => :conventional,   # A normal user record in a leaf page.
        1 => :node_pointer,   # A node pointer in a non-leaf page.
        2 => :infimum,        # The system "infimum" record.
        3 => :supremum,       # The system "supremum" record.
      }.freeze

      # The size (in bytes) of the record pointers in each page directory slot.
      PAGE_DIR_SLOT_SIZE = 2

      # The minimum number of records "owned" by each record with an entry in
      # the page directory.
      PAGE_DIR_SLOT_MIN_N_OWNED = 4

      # The maximum number of records "owned" by each record with an entry in
      # the page directory.
      PAGE_DIR_SLOT_MAX_N_OWNED = 8

      attr_writer :record_describer

      # Return the byte offset of the start of the "index" page header, which
      # immediately follows the "fil" header.
      def pos_index_header
        pos_page_body
      end

      # The size of the "index" header.
      def size_index_header
        2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 8 + 2 + 8
      end

      # Return the byte offset of the start of the "fseg" header, which immediately
      # follows the "index" header.
      def pos_fseg_header
        pos_index_header + size_index_header
      end

      # The size of the "fseg" header.
      def size_fseg_header
        2 * Innodb::FsegEntry::SIZE
      end

      # Return the size of the header for each record.
      def size_record_header
        case page_header[:format]
        when :compact
          RECORD_NEXT_SIZE + RECORD_COMPACT_BITS_SIZE
        when :redundant
          RECORD_NEXT_SIZE + RECORD_REDUNDANT_BITS_SIZE
        end
      end

      # The size of the additional data structures in the header of the system
      # records, which is just 1 byte in redundant format to store the offset
      # of the end of the field. This is needed specifically here since we need
      # to be able to calculate the fixed positions of these system records.
      def size_mum_record_header_additional
        case page_header[:format]
        when :compact
          0 # No additional data is stored in compact format.
        when :redundant
          1 # A 1-byte offset for 1 field is stored in redundant format.
        end
      end

      # The size of the data from the supremum or infimum records.
      def size_mum_record
        8
      end

      # Return the byte offset of the start of the "origin" of the infimum record,
      # which is always the first record in the singly-linked record chain on any
      # page, and represents a record with a "lower value than any possible user
      # record". The infimum record immediately follows the page header.
      def pos_infimum
        pos_records +
          size_record_header +
          size_mum_record_header_additional
      end

      # Return the byte offset of the start of the "origin" of the supremum record,
      # which is always the last record in the singly-linked record chain on any
      # page, and represents a record with a "higher value than any possible user
      # record". The supremum record immediately follows the infimum record.
      def pos_supremum
        pos_infimum +
          size_record_header +
          size_mum_record_header_additional +
          size_mum_record
      end

      # Return the byte offset of the start of records within the page (the
      # position immediately after the page header).
      def pos_records
        size_fil_header +
          size_index_header +
          size_fseg_header
      end

      # Return the byte offset of the start of the user records in a page, which
      # immediately follows the supremum record.
      def pos_user_records
        pos_supremum + size_mum_record
      end

      # The position of the page directory, which starts at the "fil" trailer and
      # grows backwards from there.
      def pos_directory
        pos_fil_trailer
      end

      # The amount of space consumed by the page header.
      def header_space
        # The end of the supremum system record is the beginning of the space
        # available for user records.
        pos_user_records
      end

      # The number of directory slots in use.
      def directory_slots
        page_header[:n_dir_slots]
      end

      # The amount of space consumed by the page directory.
      def directory_space
        directory_slots * PAGE_DIR_SLOT_SIZE
      end

      # The amount of space consumed by the trailers in the page.
      def trailer_space
        size_fil_trailer
      end

      # Return the amount of free space in the page.
      def free_space
        page_header[:garbage_size] +
          (size - size_fil_trailer - directory_space - page_header[:heap_top])
      end

      # Return the amount of used space in the page.
      def used_space
        size - free_space
      end

      # Return the amount of space occupied by records in the page.
      def record_space
        used_space - header_space - directory_space - trailer_space
      end

      # A helper to calculate the amount of space consumed per record.
      def space_per_record
        page_header.n_recs.positive? ? (record_space.to_f / page_header.n_recs) : 0
      end

      # Return the "index" header.
      def page_header
        @page_header ||= cursor(pos_index_header).name("index") do |c|
          index = PageHeader.new(
            n_dir_slots: c.name("n_dir_slots") { c.read_uint16 },
            heap_top: c.name("heap_top") { c.read_uint16 },
            n_heap_format: c.name("n_heap_format") { c.read_uint16 },
            garbage_offset: c.name("garbage_offset") { c.read_uint16 },
            garbage_size: c.name("garbage_size") { c.read_uint16 },
            last_insert_offset: c.name("last_insert_offset") { c.read_uint16 },
            direction: c.name("direction") { PAGE_DIRECTION[c.read_uint16] },
            n_direction: c.name("n_direction") { c.read_uint16 },
            n_recs: c.name("n_recs") { c.read_uint16 },
            max_trx_id: c.name("max_trx_id") { c.read_uint64 },
            level: c.name("level") { c.read_uint16 },
            index_id: c.name("index_id") { c.read_uint64 }
          )

          index.n_heap = index.n_heap_format & ((2**15) - 1)
          index.format = (index.n_heap_format & (1 << 15)).zero? ? :redundant : :compact

          index
        end
      end

      def_delegator :page_header, :index_id
      def_delegator :page_header, :level
      def_delegator :page_header, :n_recs, :records
      def_delegator :page_header, :garbage_offset

      # A helper function to identify root index pages; they must be the only pages
      # at their level.
      def root?
        prev.nil? && self.next.nil?
      end

      # A helper function to identify leaf index pages.
      def leaf?
        level.zero?
      end

      # A helper to determine if an this page is part of an insert buffer index.
      def ibuf_index?
        index_id == Innodb::IbufIndex::INDEX_ID
      end

      # Return the "fseg" header.
      def fseg_header
        @fseg_header ||= cursor(pos_fseg_header).name("fseg") do |c|
          FsegHeader.new(
            leaf: c.name("fseg[leaf]") { Innodb::FsegEntry.get_inode(@space, c) },
            internal: c.name("fseg[internal]") { Innodb::FsegEntry.get_inode(@space, c) }
          )
        end
      end

      # Return the header from a record.
      def record_header(cursor)
        origin = cursor.position
        header = RecordHeader.new
        cursor.backward.name("header") do |c|
          case page_header.format
          when :compact
            # The "next" pointer is a relative offset from the current record.
            header.next = c.name("next") { origin + c.read_sint16 }

            # Fields packed in a 16-bit integer (LSB first):
            #   3 bits for type
            #   13 bits for heap_number
            bits1 = c.name("bits1") { c.read_uint16 }
            header.type = RECORD_TYPES[bits1 & 0x07]
            header.heap_number = (bits1 & 0xfff8) >> 3
          when :redundant
            # The "next" pointer is an absolute offset within the page.
            header.next = c.name("next") { c.read_uint16 }

            # Fields packed in a 24-bit integer (LSB first):
            #   1 bit for offset_size (0 = 2 bytes, 1 = 1 byte)
            #   10 bits for n_fields
            #   13 bits for heap_number
            bits1 = c.name("bits1") { c.read_uint24 }
            header.offset_size = (bits1 & 1).zero? ? 2 : 1
            header.n_fields = (bits1 & (((1 << 10) - 1) << 1)) >> 1
            header.heap_number = (bits1 & (((1 << 13) - 1) << 11)) >> 11
          end

          # Fields packed in an 8-bit integer (LSB first):
          #   4 bits for n_owned
          #   4 bits for flags
          bits2 = c.name("bits2") { c.read_uint8 }
          header.n_owned = bits2 & 0x0f
          header.info_flags = (bits2 & 0xf0) >> 4

          case page_header.format
          when :compact
            record_header_compact_additional(header, cursor)
          when :redundant
            record_header_redundant_additional(header, cursor)
          end

          header.length = origin - cursor.position
        end

        header
      end

      # Read additional header information from a compact format record header.
      def record_header_compact_additional(header, cursor)
        case header.type
        when :conventional, :node_pointer
          # The variable-length part of the record header contains a
          # bit vector indicating NULL fields and the length of each
          # non-NULL variable-length field.
          if record_format
            header.nulls = cursor.name("nulls") { record_header_compact_null_bitmap(cursor) }
            header.lengths, header.externs = cursor.name("lengths_and_externs") do
              record_header_compact_variable_lengths_and_externs(cursor, header.nulls)
            end
          end
        end
      end

      # Return an array indicating which fields are null.
      def record_header_compact_null_bitmap(cursor)
        fields = record_fields

        # The number of bits in the bitmap is the number of nullable fields.
        size = fields.count(&:nullable?)

        # There is no bitmap if there are no nullable fields.
        return [] unless size.positive?

        # TODO: This is really ugly.
        null_bit_array = cursor.read_bit_array(size).reverse!

        # For every nullable field, select the ones which are actually null.
        fields.select { |f| f.nullable? && (null_bit_array.shift == 1) }.map(&:name)
      end

      # Return an array containing an array of the length of each variable-length
      # field and an array indicating which fields are stored externally.
      def record_header_compact_variable_lengths_and_externs(cursor, nulls)
        fields = (record_format[:key] + record_format[:row])

        lengths = {}
        externs = []

        # For each non-NULL variable-length field, the record header contains
        # the length in one or two bytes.
        fields.each do |f|
          next if !f.variable? || nulls.include?(f.name)

          len = cursor.read_uint8
          ext = false

          # Two bytes are used only if the length exceeds 127 bytes and the
          # maximum length exceeds 255 bytes (or the field is a BLOB type).
          if len > 127 && (f.blob? || f.data_type.length > 255)
            ext = (0x40 & len) != 0
            len = ((len & 0x3f) << 8) + cursor.read_uint8
          end

          lengths[f.name] = len
          externs << f.name if ext
        end

        [lengths, externs]
      end

      # Read additional header information from a redundant format record header.
      def record_header_redundant_additional(header, cursor)
        lengths = []
        nulls = []
        externs = []

        field_offsets = record_header_redundant_field_end_offsets(header, cursor)

        this_field_offset = 0
        field_offsets.each do |n|
          case header.offset_size
          when 1
            next_field_offset = (n & RECORD_REDUNDANT_OFF1_OFFSET_MASK)
            lengths << (next_field_offset - this_field_offset)
            nulls   << ((n & RECORD_REDUNDANT_OFF1_NULL_MASK) != 0)
            externs << false
          when 2
            next_field_offset = (n & RECORD_REDUNDANT_OFF2_OFFSET_MASK)
            lengths << (next_field_offset - this_field_offset)
            nulls   << ((n & RECORD_REDUNDANT_OFF2_NULL_MASK) != 0)
            externs << ((n & RECORD_REDUNDANT_OFF2_EXTERN_MASK) != 0)
          end
          this_field_offset = next_field_offset
        end

        # If possible, refer to fields by name rather than position for
        # better formatting (i.e. pp).
        if record_format
          header.lengths = {}
          header.nulls = []
          header.externs = []

          record_fields.each do |f|
            header.lengths[f.name] = lengths[f.position]
            header.nulls << f.name if nulls[f.position]
            header.externs << f.name if externs[f.position]
          end
        else
          header.lengths = lengths
          header.nulls = nulls
          header.externs = externs
        end
      end

      # Read field end offsets from the provided cursor for each field as counted
      # by n_fields.
      def record_header_redundant_field_end_offsets(header, cursor)
        header.n_fields.times.map do |n|
          cursor.name("field_end_offset[#{n}]") { cursor.read_uint_by_size(header.offset_size) }
        end
      end

      # Parse and return simple fixed-format system records, such as InnoDB's
      # internal infimum and supremum records.
      def system_record(offset)
        cursor(offset).name("record[#{offset}]") do |c|
          header = c.peek { record_header(c) }
          Innodb::Record.new(
            self,
            SystemRecord.new(
              offset: offset,
              header: header,
              next: header.next,
              data: c.name("data") { c.read_bytes(size_mum_record) },
              length: c.position - offset
            )
          )
        end
      end

      # Return the infimum record on a page.
      def infimum
        @infimum ||= system_record(pos_infimum)
      end

      # Return the supremum record on a page.
      def supremum
        @supremum ||= system_record(pos_supremum)
      end

      def make_record_describer
        if space.innodb_system.data_dictionary && index_id && !ibuf_index?
          @record_describer = space.innodb_system
                                   .data_dictionary
                                   .indexes
                                   .find(innodb_index_id: index_id)
                                   .record_describer
        elsif space
          @record_describer = space.record_describer
        end
      end

      def record_describer
        @record_describer ||= make_record_describer
      end

      # Return a set of field objects that describe the record.
      def make_record_description
        position = (0..RECORD_MAX_N_FIELDS).each
        description = record_describer.description
        fields = { type: description[:type], key: [], sys: [], row: [] }

        description[:key].each do |field|
          fields[:key] << Innodb::Field.new(position.next, field[:name], *field[:type])
        end

        # If this is a leaf page of the clustered index, read InnoDB's internal
        # fields, a transaction ID and roll pointer.
        if leaf? && fields[:type] == :clustered
          [["DB_TRX_ID", :TRX_ID], ["DB_ROLL_PTR", :ROLL_PTR]].each do |name, type|
            fields[:sys] << Innodb::Field.new(position.next, name, type, :NOT_NULL)
          end
        end

        # If this is a leaf page of the clustered index, or any page of a
        # secondary index, read the non-key fields.
        if (leaf? && fields[:type] == :clustered) || (fields[:type] == :secondary)
          description[:row].each do |field|
            fields[:row] << Innodb::Field.new(position.next, field[:name], *field[:type])
          end
        end

        fields
      end

      # Return (and cache) the record format provided by an external class.
      def record_format
        @record_format ||= make_record_description if record_describer
      end

      # Returns the (ordered) set of fields that describe records in this page.
      def record_fields
        record_format.values_at(:key, :sys, :row).flatten.sort_by(&:position) if record_format
      end

      # Parse and return a record at a given offset.
      def record(offset)
        return nil unless offset
        return infimum if offset == pos_infimum
        return supremum if offset == pos_supremum

        cursor(offset).forward.name("record[#{offset}]") do |c|
          # There is a header preceding the row itself, so back up and read it.
          header = c.peek { record_header(c) }

          this_record = UserRecord.new(
            format: page_header.format,
            offset: offset,
            header: header,
            next: header.next.zero? ? nil : header.next
          )

          if record_format
            this_record.type = record_format[:type]

            # Used to indicate whether a field is part of key/row/sys.
            # TODO: There's probably a better way to do this.
            fmap = %i[key row sys].each_with_object({}) do |k, h|
              this_record[k] = []
              record_format[k].each { |f| h[f.position] = k }
            end

            # Read the fields present in this record.
            record_fields.each do |f|
              p = fmap[f.position]
              c.name("#{p}[#{f.name}]") do
                this_record[p] << FieldDescriptor.new(
                  name: f.name,
                  type: f.data_type.name,
                  value: f.value(c, this_record),
                  extern: f.extern(c, this_record)
                )
              end
            end

            # If this is a node (non-leaf) page, it will have a child page number
            # (or "node pointer") stored as the last field.
            this_record.child_page_number = c.name("child_page_number") { c.read_uint32 } unless leaf?

            this_record.length = c.position - offset

            # Add system field accessors for convenience.
            this_record.sys.each do |f|
              case f[:name]
              when "DB_TRX_ID"
                this_record.transaction_id = f[:value]
              when "DB_ROLL_PTR"
                this_record.roll_pointer = f[:value]
              end
            end
          end

          Innodb::Record.new(self, this_record)
        end
      end

      # Return an array of row offsets for all entries in the page directory.
      def directory
        @directory ||= cursor(pos_directory).backward.name("page_directory") do |c|
          directory_slots.times.map { |n| c.name("slot[#{n}]") { c.read_uint16 } }
        end
      end

      # Return the slot number of the provided offset in the page directory, or nil
      # if the offset is not present in the page directory.
      def offset_directory_slot(offset)
        directory.index(offset)
      end

      # Return the slot number of the provided record in the page directory, or nil
      # if the record is not present in the page directory.
      def record_directory_slot(this_record)
        offset_directory_slot(this_record.offset)
      end

      # Return the slot number for the page directory entry which "owns" the
      # provided record. This will be either the record itself, or the nearest
      # record with an entry in the directory and a value greater than the record.
      def directory_slot_for_record(this_record)
        slot = record_directory_slot(this_record)
        return slot if slot

        search_cursor = record_cursor(this_record.next)
        raise "Could not position cursor" unless search_cursor

        while (rec = search_cursor.record)
          slot = record_directory_slot(rec)
          return slot if slot
        end

        record_directory_slot(supremum)
      end

      def each_directory_offset
        return enum_for(:each_directory_offset) unless block_given?

        directory.each do |offset|
          yield offset unless [pos_infimum, pos_supremum].include?(offset)
        end
      end

      def each_directory_record
        return enum_for(:each_directory_record) unless block_given?

        each_directory_offset do |offset|
          yield record(offset)
        end
      end

      # A class for cursoring through records starting from an arbitrary point.
      class RecordCursor
        def initialize(page, offset, direction)
          Innodb::Stats.increment :page_record_cursor_create

          @initial = true
          @page = page
          @direction = direction
          @record = initial_record(offset)
        end

        def initial_record(offset)
          case offset
          when :min
            @page.min_record
          when :max
            @page.max_record
          else
            # Offset is a byte offset of a record (hopefully).
            @page.record(offset)
          end
        end

        # Return the next record, and advance the cursor. Return nil when the
        # end of records (supremum) is reached.
        def next_record
          Innodb::Stats.increment :page_record_cursor_next_record

          rec = @page.record(@record.next)

          # The garbage record list's end is self-linked, so we must check for
          # both supremum and the current record's offset.
          if rec == @page.supremum || rec.offset == @record.offset
            # We've reached the end of the linked list at supremum.
            nil
          else
            @record = rec
          end
        end

        # Return the previous record, and advance the cursor. Return nil when the
        # end of records (infimum) is reached.
        def prev_record
          Innodb::Stats.increment :page_record_cursor_prev_record

          slot = @page.directory_slot_for_record(@record)
          raise "Could not find slot for record" unless slot

          search_cursor = @page.record_cursor(@page.directory[slot - 1])
          raise "Could not position search cursor" unless search_cursor

          while (rec = search_cursor.record) && rec.offset != @record.offset
            next unless rec.next == @record.offset

            return if rec == @page.infimum

            return @record = rec
          end
        end

        # Return the next record in the order defined when the cursor was created.
        def record
          if @initial
            @initial = false
            return @record
          end

          case @direction
          when :forward
            next_record
          when :backward
            prev_record
          end
        end

        # Iterate through all records in the cursor.
        def each_record
          return enum_for(:each_record) unless block_given?

          while (rec = record)
            yield rec
          end
        end
      end

      # Return a RecordCursor starting at offset.
      def record_cursor(offset = :min, direction = :forward)
        RecordCursor.new(self, offset, direction)
      end

      def record_if_exists(offset)
        each_record do |rec|
          return rec if rec.offset == offset
        end
      end

      # Return the minimum record on this page.
      def min_record
        min = record(infimum.next)
        min if min != supremum
      end

      # Return the maximum record on this page.
      def max_record
        # Since the records are only singly-linked in the forward direction, in
        # order to do find the last record, we must create a cursor and walk
        # backwards one step.
        max_cursor = record_cursor(supremum.offset, :backward)
        raise "Could not position cursor" unless max_cursor

        # Note the deliberate use of prev_record rather than record; we want
        # to skip over supremum itself.
        max = max_cursor.prev_record
        max if max != infimum
      end

      # Search for a record within a single page, and return either a perfect
      # match for the key, or the last record closest to they key but not greater
      # than the key. (If an exact match is desired, compare_key must be used to
      # check if the returned record matches. This makes the function useful for
      # search in both leaf and non-leaf pages.)
      def linear_search_from_cursor(search_cursor, key)
        Innodb::Stats.increment :linear_search_from_cursor

        this_rec = search_cursor.record

        if Innodb.debug?
          puts "linear_search_from_cursor: page=%i, level=%i, start=(%s)" % [
            offset,
            level,
            this_rec && this_rec.key_string,
          ]
        end

        # Iterate through all records until finding either a matching record or
        # one whose key is greater than the desired key.
        while this_rec && (next_rec = search_cursor.record)
          Innodb::Stats.increment :linear_search_from_cursor_record_scans

          if Innodb.debug?
            puts "linear_search_from_cursor: page=%i, level=%i, current=(%s)" % [
              offset,
              level,
              this_rec.key_string,
            ]
          end

          # If we reach supremum, return the last non-system record we got.
          return this_rec if next_rec.header.type == :supremum

          return this_rec if this_rec.compare_key(key).negative?

          # The desired key is either an exact match for this_rec or is greater
          # than it but less than next_rec. If this is a non-leaf page, that
          # will mean that the record will fall on the leaf page this node
          # pointer record points to, if it exists at all.
          return this_rec if !this_rec.compare_key(key).negative? && next_rec.compare_key(key).negative?

          this_rec = next_rec
        end

        this_rec
      end

      # Search or a record within a single page using the page directory to limit
      # the number of record comparisons required. Once the last page directory
      # entry closest to but not greater than the key is found, fall back to
      # linear search using linear_search_from_cursor to find the closest record
      # whose key is not greater than the desired key. (If an exact match is
      # desired, the returned record must be checked in the same way as the above
      # linear_search_from_cursor function.)
      def binary_search_by_directory(dir, key)
        Innodb::Stats.increment :binary_search_by_directory

        return if dir.empty?

        # Split the directory at the mid-point (using integer math, so the division
        # is rounding down). Retrieve the record that sits at the mid-point.
        mid = ((dir.size - 1) / 2)
        rec = record(dir[mid])
        return unless rec

        if Innodb.debug?
          puts "binary_search_by_directory: page=%i, level=%i, dir.size=%i, dir[%i]=(%s)" % [
            offset,
            level,
            dir.size,
            mid,
            rec.key_string,
          ]
        end

        # The mid-point record was the infimum record, which is not comparable with
        # compare_key, so we need to just linear scan from here. If the mid-point
        # is the beginning of the page there can't be many records left to check
        # anyway.
        return linear_search_from_cursor(record_cursor(rec.next), key) if rec.header.type == :infimum

        # Compare the desired key to the mid-point record's key.
        case rec.compare_key(key)
        when 0
          # An exact match for the key was found. Return the record.
          Innodb::Stats.increment :binary_search_by_directory_exact_match
          rec
        when +1
          # The mid-point record's key is less than the desired key.
          if dir.size > 2
            # There are more entries remaining from the directory, recurse again
            # using binary search on the right half of the directory, which
            # represents values greater than or equal to the mid-point record's
            # key.
            Innodb::Stats.increment :binary_search_by_directory_recurse_right
            binary_search_by_directory(dir[mid...dir.size], key)
          else
            next_rec = record(dir[mid + 1])
            next_key = next_rec&.compare_key(key)
            if dir.size == 1 || next_key == -1 || next_key.zero?
              # This is the last entry remaining from the directory, or our key is
              # greater than rec and less than rec+1's key. Use linear search to
              # find the record starting at rec.
              Innodb::Stats.increment :binary_search_by_directory_linear_search
              linear_search_from_cursor(record_cursor(rec.offset), key)
            elsif next_key == +1
              Innodb::Stats.increment :binary_search_by_directory_linear_search
              linear_search_from_cursor(record_cursor(next_rec.offset), key)
            end
          end
        when -1
          # The mid-point record's key is greater than the desired key.
          if dir.size == 1
            # If this is the last entry remaining from the directory, we didn't
            # find anything workable.
            Innodb::Stats.increment :binary_search_by_directory_empty_result
            nil
          else
            # Recurse on the left half of the directory, which represents values
            # less than the mid-point record's key.
            Innodb::Stats.increment :binary_search_by_directory_recurse_left
            binary_search_by_directory(dir[0...mid], key)
          end
        end
      end

      # Iterate through all records.
      def each_record
        return enum_for(:each_record) unless block_given?

        c = record_cursor(:min)

        while (rec = c.record)
          yield rec
        end

        nil
      end

      # Iterate through all records in the garbage list.
      def each_garbage_record
        return enum_for(:each_garbage_record) unless block_given?
        return if garbage_offset.zero?

        c = record_cursor(garbage_offset)

        while (rec = c.record)
          yield rec
        end

        nil
      end

      # Iterate through all child pages of a node (non-leaf) page, which are
      # stored as records with the child page number as the last field in the
      # record.
      def each_child_page
        return if leaf?

        return enum_for(:each_child_page) unless block_given?

        each_record do |rec|
          yield rec.child_page_number, rec.key
        end

        nil
      end

      def each_region(&block)
        return enum_for(:each_region) unless block_given?

        super

        yield Region.new(
          offset: pos_index_header,
          length: size_index_header,
          name: :index_header,
          info: "Index Header"
        )

        yield Region.new(
          offset: pos_fseg_header,
          length: size_fseg_header,
          name: :fseg_header,
          info: "File Segment Header"
        )

        yield Region.new(
          offset: pos_infimum - 5,
          length: size_mum_record + 5,
          name: :infimum,
          info: "Infimum"
        )

        yield Region.new(
          offset: pos_supremum - 5,
          length: size_mum_record + 5,
          name: :supremum,
          info: "Supremum"
        )

        directory_slots.times do |n|
          yield Region.new(
            offset: pos_directory - (n * 2),
            length: 2,
            name: :directory,
            info: "Page Directory"
          )
        end

        each_garbage_record do |record|
          yield Region.new(
            offset: record.offset - record.header.length,
            length: record.length + record.header.length,
            name: :garbage,
            info: "Garbage"
          )
        end

        each_record do |record|
          yield Region.new(
            offset: record.offset - record.header.length,
            length: record.header.length,
            name: :record_header,
            info: "Record Header"
          )

          yield Region.new(
            offset: record.offset,
            length: record.length || 1,
            name: :record_data,
            info: "Record Data"
          )
        end

        nil
      end

      # Dump the contents of a page for debugging purposes.
      def dump
        super

        puts "page header:"
        pp page_header
        puts

        puts "fseg header:"
        pp fseg_header
        puts

        puts "sizes:"
        puts "  %-15s%5i" % ["header", header_space]
        puts "  %-15s%5i" % ["trailer", trailer_space]
        puts "  %-15s%5i" % ["directory", directory_space]
        puts "  %-15s%5i" % ["free", free_space]
        puts "  %-15s%5i" % ["used", used_space]
        puts "  %-15s%5i" % ["record", record_space]
        puts "  %-15s%5.2f" % ["per record", space_per_record]
        puts

        puts "page directory:"
        pp directory
        puts

        puts "system records:"
        pp infimum.record
        pp supremum.record
        puts

        if ibuf_index?
          puts "(records not dumped due to this being an insert buffer index)"
        elsif !record_describer
          puts "(records not dumped due to missing record describer or data dictionary)"
        else
          puts "garbage records:"
          each_garbage_record do |rec|
            pp rec.record
            puts
          end
          puts

          puts "records:"
          each_record do |rec|
            pp rec.record
            puts
          end
        end
        puts
      end
    end
  end
end
