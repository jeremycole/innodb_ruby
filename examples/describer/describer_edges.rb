# -*- encoding : utf-8 -*-
class Innodb::RecordDescriber::Edges < Innodb::RecordDescriber
  def self.cursor_sendable_description(page)
    bytes_per_record = (page.record_space / page.page_header[:n_recs])
    case
    when [48, 26].include?(bytes_per_record) # Clustered Key
      {
        # PRIMARY KEY (source_id, state, position)
        :type => :clustered,
        :key => [
          [:BIGINT,  :UNSIGNED, :NOT_NULL],  # source_id
          [:TINYINT, :NOT_NULL],             # state
          [:BIGINT,  :NOT_NULL],             # position
        ],
        :row => [
          [:INT,     :UNSIGNED, :NOT_NULL],  # updated_at
          [:BIGINT,  :UNSIGNED, :NOT_NULL],  # destination_id
          [:TINYINT, :UNSIGNED, :NOT_NULL],  # count
        ],
      }
    when [30, 34].include?(bytes_per_record) # Secondary Key
      {
        # INDEX (source_id, destination_id)
        :type => :secondary,
        :key => [
          [:BIGINT, :UNSIGNED, :NOT_NULL],   # source_id
          [:BIGINT, :UNSIGNED, :NOT_NULL],   # destination_id
        ],
        # PKV ([source_id], state, position)
        :row => [
          [:TINYINT, :UNSIGNED, :NOT_NULL],  # state
          [:TINYINT, :UNSIGNED, :NOT_NULL],  # position
        ],
      }
    end
  end
end
