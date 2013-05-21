# -*- encoding : utf-8 -*-
class Innodb::RecordDescriber::Example < Innodb::RecordDescriber
  def self.cursor_sendable_description(page)
    case 1
    when 0
      {
        :type => :clustered,
        :key => [
          [:INT, :UNSIGNED, :NOT_NULL],
        ],
        :row => [
          [:TINYINT],
          [:SMALLINT],
          [:MEDIUMINT],
          [:INT],
          [:BIGINT],
          [:TINYINT,  :UNSIGNED],
          [:SMALLINT, :UNSIGNED],
          [:MEDIUMINT,:UNSIGNED],
          [:INT,      :UNSIGNED],
          [:BIGINT,   :UNSIGNED],
        ],
      }
    when 1
      {
        :type => :clustered,
        :key => [
          ["INT", :UNSIGNED, :NOT_NULL],
          ["VARCHAR(16)", :NOT_NULL],
        ],
        :row => [
          ["INT"],
          ["VARCHAR(64)"],
          ["INT", :NOT_NULL],
          ["VARCHAR(128)", :NOT_NULL],
          ["INT"],
          ["VARCHAR(512)"],
        ],
      }
    end
  end
end
