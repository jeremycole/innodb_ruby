class Innodb::RecordDescriber::Edges < Innodb::RecordDescriber
  def self.cursor_sendable_description(page)
    bytes_per_record = (page.record_space / page.page_header[:n_recs])
    case
    when [48, 26].include?(bytes_per_record) # Clustered Key
      {
        # PRIMARY KEY (source_id, state, position)
        :type => :clustered,
        :key => [
          [:get_uint64],    # source_id
          [:get_i_sint8],   # state
          [:get_i_sint64],  # position
        ],
        :row => [
          [:get_uint32],    # updated_at
          [:get_uint64],    # destination_id
          [:get_uint8],     # count
        ],
      }
    when [30, 34].include?(bytes_per_record) # Secondary Key
      {
        # INDEX (source_id, destination_id)
        :type => :secondary,
        :key => [
          [:get_uint64],    # source_id
          [:get_uint64],    # destination_id
        ],
        # PKV ([source_id], state, position)
        :row => [
          [:get_i_sint8],   # state
          [:get_i_sint64],  # position
        ],
      }
    end
  end
end
