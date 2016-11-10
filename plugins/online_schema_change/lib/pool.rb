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


      raise "Not all nodes are running, failing to continue." unless all_nodes_running?
      all_nodes_in_pool.each { |node|
        node.create_user username, password
        node.grant_privileges username, '*', 'PROCESS', 'REPLICATION CLIENT', 'REPLICATION SLAVE', 'SUPER'
        node.grant_privileges username, database, 'ALL PRIVILEGES'
        node.grant_privileges username, Percona::DSNTable::SCHEMA_NAME, 'ALL PRIVILEGES'
      }

      return username, password
    end

    def drop_ptosc_user
      trigger_count = count_ptosc_triggers
      if trigger_count > 0
        output "WHOA BUDDY! There should be _no triggers_ for the ptosc user, and yet there are!".red
        output "This indicates a bug in Jetpants, and we're giving right up.".red
        raise "pt-osc has triggers, but we should have already dropped them!".red
      end

      username = PT_OSC_USERNAME
      all_nodes_in_pool.reverse.each { |node|
        node.drop_user username
      }
    end

    def alter_table(database, table, alter, dry_run=true, force=false, skip_rename=false, arbitrary_options=[])
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

        clean_up_tables_from_prior_ptosc_alter! database, table

        dsn_table = Percona::DSNTable.new(master)
        dsn_table.create_with_nodes(active_slaves)

        username, password = create_ptosc_user database
        ptosc = PTOSC.new(self, database, table, alter, username, password, {
                            :delayed_rename => skip_rename,
                            :slave_monitor_dsn => dsn_table.dsn
                          })

        unless arbitrary_options.empty?
          ptosc.arbitrary_options = arbitrary_options
        end

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

            unless agree('Would you like to continue? (YES/no)')
              output "Skipping the execution! Cleaning up."
              return
            end
          end

          if not ptosc_execute ptosc
            output "Failed to execute alter! Cleaning up.".red
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
          begin
            cleanup! database, table
          rescue Exception => e
            output "Captured error in cleanup: #{e}"
            output "Swallowed to allow raising errors from ensure..."
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

    def clean_up_tables_from_prior_ptosc_alter!(database, table)
      database ||= app_schema

      cruft = list_tables_from_prior_ptosc_alter(database, table)
      if cruft.length > 0
        output "The following old tables exist and should be cleaned up before we continue:".red
        output "Old tables: #{cruft.join(', ')}".red

        cruft.each do |crufty_table|
          catch_up_slaves_then "Drop crufty table '#{crufty_table}'" do
            master.mysql_root_cmd("USE #{database}; DROP TABLE IF EXISTS #{crufty_table}")
          end
        end
      end
    end

    def list_tables_from_prior_ptosc_alter(database, table)
      query = <<-END
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA='#{database}'
          AND (
               TABLE_NAME LIKE '%#{table}_new'
            OR TABLE_NAME = '_#{table}_old'
            OR TABLE_NAME LIKE '%#{table}_new_tmp'
          )
        ;
      END

      master.query_return_array(query).map { |row| row[:TABLE_NAME] }
    end

    def rename_table(database, orig_table, copy_table)
      if Jetpants.plugin_enabled? 'jetpants_collins'
        raise "Collins doesn't indicate we need a rename? #{@name}" unless collins_check_needs_rename?
      end

      catch_up_slaves_then "Execute rename" do
        master.mysql_root_cmd("USE #{database}; RENAME TABLE #{copy_table} TO #{copy_table}_tmp, #{orig_table} TO _#{orig_table}_old, #{copy_table}_tmp TO #{orig_table}")
      end

      cleanup! database, orig_table
    end

    def cleanup! database, table
      dsntable = Percona::DSNTable.new(master)
      output "Note: The cleanup must complete the following steps:"
      output " - Drop #{Percona::DSNTable::SCHEMA_NAME}.#{Percona::DSNTable::TABLE_NAME}".red
      output " - Drop the triggers".red
      output " - Drop the ptosc users (Note: _AFTER_ all slaves are caught up!)".red
      output " - Clean up the collins state".red
      output " "
      output "If these things don't happen due to error or Ctrl-C, please clean up manually"

      catch_up_slaves_then "Clean up the dsn table" do
        dsntable.destroy!
      end

      catch_up_slaves_then "Drop triggers" do
        master.drop_online_schema_change_triggers database, table
      end

      catch_up_slaves_then "Clean up pt-osc user" do
        # The following wait is _very_ important to be certain all slaves are fully caught up and have
        # already replicated the dropping of the triggers
        drop_ptosc_user
      end

      clean_up_tables_from_prior_ptosc_alter! database, table

      if Jetpants.plugin_enabled? 'jetpants_collins'
        collins_set_can_be_altered!
      end
      output "Completed cleaning up collins".green
    end

    def count_ptosc_triggers
      query = <<-END
        select count(*)
        from INFORMATION_SCHEMA.TRIGGERS
        where DEFINER LIKE "#{PT_OSC_USERNAME}@%"
      END

      master.query_return_first_value(query)
    end

    def catch_up_slaves_then msg
      begin
        wait_for_all_slaves "Carefully: #{msg}".red

        unless agree("Do you want to immediately: #{msg}? (YES/no)")
          raise "Definitely did not want to run this!"
        end

        yield

        output "Finished: #{msg}".green
      rescue
        output "Failed to run #{msg}!".red
        raise
      end
    end

    def wait_for_all_slaves msg=nil
      output "Checking to see if all nodes are running...".green
      until all_nodes_running?
        output "Waiting on all nodes to be running..."
        output msg unless msg.empty?
        sleep 5
      end

      output "Checking to see if all slaves are caught up...".green
      until all_slaves_caught_up?
        output "Waiting on all slaves to catch up ..."
        output msg unless msg.empty?

        sleep 5
      end
    end

    def all_nodes_running?
      all_nodes_in_pool.concurrent_map { |node|
        ret = node.running?
        output "Warning: #{node} not running!".red unless ret

        ret
      }.all?
    end

    def all_nodes_in_pool
      nodes = slaves_according_to_collins
      nodes << master
      nodes
    end

    def all_slaves_caught_up?
      slaves_according_to_collins.concurrent_map { |slave|
        slave.catch_up_to_master
      }.all?
    end
  end
end
