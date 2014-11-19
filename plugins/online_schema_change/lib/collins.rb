# monkeypatches to add Collins integration

module Jetpants
  class Pool
    collins_attr_accessor :online_schema_change

    # update collins for tracking alters, so there is only one running at a time
    def update_collins_for_alter(database, table, alter)
      meta = {
          'running' => true,
          'started' => Time.now.to_i,
          'database' => database,
          'table' => table,
          'alter' => alter
      }
      self.collins_online_schema_change = JSON.pretty_generate(meta)
    end

    # check if a alter is already running
    def check_collins_for_alter
      return true if self.collins_online_schema_change.empty?
      meta = JSON.parse(self.collins_online_schema_change)

      !meta['running']
    end

    # clean up collins after alter
    def clean_up_collins_for_alter
      self.collins_online_schema_change = ''
    end

  end
end
