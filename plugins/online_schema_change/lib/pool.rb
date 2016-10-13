# JetCollins monkeypatches to add Collins integration

module Jetpants
  class Pool

    # Created as a class constant because the user interface expects this value to be used across
    # calls. ie: an operator will call alter_table, and then call alter_table_rename.
    #
    # This constant is a constant to imply that it shouldn't be generated without rethinking this,
    # and in order to hold this documentation.
    #
    # Note: This constant is only intended to be used in the functions the UI would call, ie:
    #
    # - create_ptosc_user
    # - drop_ptosc_user
    #
    # But if you're looking to _use_ the username, you should be calling either of those functions.
    PT_OSC_USERNAME = 'pt-osc'

    def create_ptosc_user database
      username = PT_OSC_USERNAME
      password = DB.random_password

      all_nodes_in_pool.each { |node|
        node.create_user username, password
        node.grant_privileges username, '*', 'PROCESS', 'REPLICATION CLIENT', 'REPLICATION SLAVE', 'SUPER'
        node.grant_privileges username, database, 'ALL PRIVILEGES'
        node.grant_privileges username, Percona::DSNTable::SCHEMA_NAME, 'ALL PRIVILEGES'
      }

      return username, password
    end

    def drop_ptosc_user
      username = PT_OSC_USERNAME
      all_nodes_in_pool.reverse.each { |node|
        node.drop_user username
      }
    end

    def alter_table(database, table, alter, dry_run=true, force=false, no_check_plan=false, skip_rename=false)
      database ||= app_schema

      # get the version of pt-online-schema-change
      pt_osc_version = `pt-online-schema-change --version`.to_s.split(' ').last.chomp rescue '0.0.0'
      if Gem::Version.new(pt_osc_version) >= Gem::Version.new('2.2.18')
        # Before 2.2.18, --version would exit 0
        raise "pt-online-schema-change executable is not available on the host" unless $?.exitstatus == 0
      else
        raise "pt-online-schema-change executable is not available on the host" unless $?.exitstatus == 1
      end

      raise "not enough space to run alter table on #{table}" unless master.has_space_for_alter?(table, database)

      if skip_rename and Gem::Version.new(pt_osc_version) < Gem::Version.new('2.2.10')
        raise "Cannot use skip_rename on #{pt_osc_version} -- must have >= 2.2.10"
      end

      if dry_run and skip_rename
        output "NOTE: skip_rename is enabled which means when we perform the migration we won't"
        output "clean up the schema change users and collins alter status. HOWEVER! because"
        output "dry_run is also enabled, we must clean up these states at the end, because you"
        output "won't be calling alter_table_rename at the end. So, when you see messages about"
        output "undoing Collins state and the utility users, be assured they won't be cleaned up"
        output "during a real run."
      end

      if Jetpants.plugin_enabled? 'jetpants_collins'
        raise "alter table already running on #{@name}" unless collins_check_can_be_altered?
      end

      clean_up_state = true
      begin
        if Jetpants.plugin_enabled? 'jetpants_collins'
          collins_set_being_altered!(database, table, alter, skip_rename)
        end

        dsn_table = Percona::DSNTable.new(master)
        dsn_table.create_with_nodes(active_slaves)

        username, password = create_ptosc_user database
        ptosc = PTOSC.new(self, database, table, alter, username, password, {
                            :no_check_plan => no_check_plan,
                            :delayed_rename => skip_rename,
                            :slave_monitor_dsn => dsn_table.dsn
                          })

        ptosc.dry_run = true
        if ptosc_execute(ptosc)

          if dry_run
            # Dry run only
            return
          end

          ptosc.dry_run = false
          # Dry run went okay
          unless force # force means skip the prompt
            # If we are not being forced, then only continue if the user types YES
            output "Dry run complete. Continuing means running the following command:"
            output " "
            output "    #{ptosc.exec_command_line.red}"
            output " "

            unless ptosc_verify_continue('Would you like to continue?')
              output "Skipping the execution! Cleaning up."
              return
            end
          end

          if not ptosc_execute ptosc
            output "Failed to execute alter! Cleaning up."
            return
          end

          if ptosc.delayed_rename?
            output "Ready for rename!"
            # The only case in which we don't want to clean up state
            clean_up_state = false

            if Jetpants.plugin_enabled? 'jetpants_collins'
              collins_set_needs_rename!
            end
          end
        end
      ensure
        if clean_up_state
          master.drop_online_schema_change_triggers database, table
          dsn_table.destroy!
          drop_ptosc_user
          if Jetpants.plugin_enabled? 'jetpants_collins'
            collins_set_can_be_altered!
          end
        end
      end
    end

    def ptosc_execute ptosc
      if ptosc.dry_run?
        display = ptosc.exec_command_line.green
      else
        display = ptosc.exec_command_line.red
      end

      output
      output "---------------------------------------------------------------------------------------"
      output "#{display}"
      output " "
      output "Note: The above command will be `exec`'d and shell expansion will not happen, and"
      output "      arguments don't need to be escaped in the same way. For this reason escaping"
      output "      may appear missing, but is actually not necessary."
      output "---------------------------------------------------------------------------------------"
      output

      ptosc.exec! do |io|
        io.each do |line|
          output line.gsub("\n", "")
        end
      end
    end

    def ptosc_verify_continue(question)
      return ask("#{question} (YES/no)") == 'YES'
    end

    # drop old table after an alter, this is because
    # we do not drop the table after an alter
    def drop_old_alter_table(database, table)
      database ||= app_schema
      master.mysql_root_cmd("USE #{database}; DROP TABLE IF EXISTS _#{table}_old")
    end

    def rename_table(database, orig_table, copy_table)
      if Jetpants.plugin_enabled? 'jetpants_collins'
        raise "Collins doesn't indicate we need a rename? #{@name}" unless collins_check_needs_rename?
      end

      wait_for_all_slaves "Note: At this point, no cleanup has taken place. You can safely Ctrl-C."

      dsntable = Percona::DSNTable.new(master)
      output "Note: The rename must complete three steps:".red
      output " - Drop #{Percona::DSNTable::SCHEMA_NAME}.#{Percona::DSNTable::TABLE_NAME}".red
      output " - Execute the rename".red
      output " - drop the triggers".red
      output " - drop the ptosc users (Note: _AFTER_ all slaves are caught up!)".red
      output " - clean up the collins state".red
      output " "
      output "If these three things don't happen due to error or Ctrl-C, please clean up manually"
      database ||= app_schema

      dsntable.destroy!
      output "Completed destruction of the dsns table".green
      master.mysql_root_cmd("USE #{database}; RENAME TABLE #{copy_table} TO #{copy_table}_tmp, #{orig_table} TO _#{orig_table}_old, #{copy_table}_tmp TO #{orig_table}")
      output "Completed the rename to be live".green
      master.drop_online_schema_change_triggers database, orig_table
      output "Completed the dropping of the pt-osc triggers".green

      # The following wait is very important to be certain all slaves are fully caught up and have
      # already replicated the dropping of the triggers
      wait_for_all_slaves <<-MSG
        Note: rename is complete, remaining tasks:
          - is to drop the pt-osc user on all nodes
          - clean up the collins state, nilling out the state
      MSG
      drop_ptosc_user
      output "Completed dropping the pt-osc user".green

      if Jetpants.plugin_enabled? 'jetpants_collins'
        collins_set_can_be_altered!
      end
      output "Completed cleaning up collins".green
    end

    def wait_for_all_slaves msg=nil
      until all_slaves_caught_up
        output "Waiting on all slaves to catch up ..."
        output msg unless msg.empty?

        sleep 5
      end
    end

    def all_nodes_in_pool
      nodes = slaves_according_to_collins
      nodes << master
      nodes
    end

    def all_slaves_caught_up
      caught_up = true

      slaves_according_to_collins.each do |slave|
        status = slave.slave_status

        if status[:slave_io_running] != "Yes"
          output "#{slave}: Slave IO thread not running".red
          caught_up = false
        end

        if status[:slave_sql_running] != "Yes"
          output "#{slave}: Slave SQL thread not running".red
          caught_up = false
        end

        behind = status[:seconds_behind_master]
        if behind == "NULL"
          raise "#{slave}: Replication is NULL seconds behind. Broken replication? Quitting with cowardice!"
        elsif behind.to_i > 0
          output "#{slave}: Replication is behind by #{slave.slave_status[:seconds_behind_master]}!"
          caught_up = false
        end
      end

      return caught_up
    end
  end
end
