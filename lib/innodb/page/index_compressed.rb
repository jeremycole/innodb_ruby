# frozen_string_literal: true

# This is horribly incomplete and broken. InnoDB compression does not
# currently work in innodb_ruby. Patches are welcome!
# (Hint hint, nudge nudge, Facebook developers!)

module Innodb
  class Page
    class IndexCompressed < Index
      specialization_for({ type: :INDEX, compressed: true })

      # The number of directory slots in use.
      def directory_slots
        page_header[:n_heap] - 2
      end

      def directory
        super.map { |n| n & 0x3fff }
      end

      def uncompressed_columns_size
        if leaf?
          if record_format && record_format[:type] == :clustered
            6 + 7 # Transaction ID + Roll Pointer
          else
            0
          end
        else
          4 # Node pointer for non-leaf pages
        end
      end

      # Return the amount of free space in the page.
      def free_space
        free_space_start =
          size - size_fil_trailer - directory_space - (uncompressed_columns_size * (page_header.n_heap - 2))
        puts 'Free space start == %04x' % [offset * size + free_space_start]
        c = cursor(free_space_start).backward
        zero_bytes = 0
        zero_bytes += 1 while c.read_uint8.zero?
        zero_bytes
        # page_header[:garbage] + (size - size_fil_trailer - directory_space - page_header[:heap_top])
      end
    end
  end
end
