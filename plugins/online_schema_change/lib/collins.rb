# monkeypatches to add Collins integration

module Jetpants
  class Pool
    collins_attr_accessor :online_schema_change


    # We're using collins to contain our implicit state machine, which looks like this:
    #
    # can_be_altered -> being_altered -> can_be_altered
    #                                 \
    #                                  -> needs_rename -> can_be_altered
    # You enter needs_rename by passing `--skip-rename` at the start, after the alter table call
    # completes according to pt-online-schema-change.
    #
    # You exit `--skip-rename` when calling `alter_table_rename`


    # check if a alter is already running
    def collins_check_can_be_altered?
      collins_osc_state['current_state'] == "can_be_altered"
    end

    # update collins for tracking alters, so there is only one running at a time
    def collins_set_being_altered!(database, table, alter, skip_rename)
      self.collins_osc_state = {
          'running' => true,
          'started' => Time.now.to_i,
          'database' => database,
          'table' => table,
          'alter' => alter,
          'current_state' => "being_altered",
          'next_state' => skip_rename ? "needs_rename" : "can_be_altered"
      }
    end

    # Transition to state: needs_rename
    def collins_set_needs_rename!
      state = collins_osc_state
      state.merge!({
        'running' => false,
        'finished' => Time.now.to_i,
        'current_state' => "needs_rename",
        'next_state' => "can_be_altered"
      })

      self.collins_osc_state = state
    end

    # check to see if we can progress to needs_rename
    def collins_check_needs_rename?
      collins_osc_state['current_state'] == "needs_rename"
    end

    # clean up collins after alter / rename
    def collins_set_can_be_altered!
      self.collins_osc_state = nil
    end

    # Helpers:
    def collins_osc_state
      if self.collins_online_schema_change.empty?
        return {
          'current_state' => "can_be_altered"
        }
      end

      return JSON.parse(self.collins_online_schema_change)
    end

    def collins_osc_state= val
      if val.nil?
        output "#{collins_osc_state['current_state']} -> can_be_altered"
        self.collins_online_schema_change = ''
      else
        output "#{collins_osc_state['current_state']} -> #{val['current_state']}"
        self.collins_online_schema_change = JSON.pretty_generate(val)
      end
    end
  end
end
