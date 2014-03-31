# -*- encoding : utf-8 -*-

# The global history of record versions implemented through undo logs.
class Innodb::History
  def initialize(innodb_system)
    @innodb_system = innodb_system
  end

  # A helper to get to the trx_sys page in the Innodb::System.
  def trx_sys
    @innodb_system.system_space.trx_sys
  end

  # A helper to get to the history_list of a given space_id and page number.
  def history_list(space_id, page_number)
    @innodb_system.space(space_id).page(page_number).history_list
  end

  # Iterate through all history lists (one per rollback segment, nominally
  # there are 128 rollback segments).
  def each_history_list
    unless block_given?
      return enum_for(:each_history_list)
    end

    trx_sys.rsegs.each do |slot|
      yield history_list(slot[:space_id], slot[:page_number])
    end
  end
end
