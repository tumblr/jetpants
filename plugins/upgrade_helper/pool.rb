require 'open3'

module Jetpants
  class Pool
    collins_attr_accessor :checksum_running
    
    # Runs pt-table-checksum on the pool.
    # Returns true if no problems found, false otherwise.
    # If problems were found, the 'checksums' table will be
    # left in the pool - the user must review and manually delete.
    def checksum_tables
      schema = master.app_schema
      success = false
      output_lines = []
      
      # check if already running, or a previous run died
      previous_run = collins_checksum_running
      previous_run = nil if previous_run == ''
      if previous_run
        run_data = JSON.parse(previous_run.downcase)  # hash with 'from_host', 'from_pid', 'timestamp'
        previous_host = run_data['from_host'].to_host
        previous_pid = run_data['from_pid'] or die 'No previous pid found in previous rundata?'
        still_running = previous_host.pid_running?(previous_pid, 'pt-table-checksum')
        raise "Checksum already in progress from #{previous_host}, pid=#{previous_pid}" if still_running
        output "Previous failed run detected, will use --resume parameter"
      end
      
      # Determine what to pass to --max-load
      master.output "Polling for normal max threads_running, please wait"
      max_threads_running = master.max_threads_running
      limit_threads_running = [(max_threads_running * 1.2).ceil, 50].max
      master.output "Found max threads_running=#{max_threads_running}, will use limit of #{limit_threads_running}"
      
      # Operate with a temporary user that has elevated permissions
      master.with_pt_checksum_user do |username, password|
        # Build command line
        command_line = ['pt-table-checksum',
          '--no-check-replication-filters',
          "--databases #{schema}",
          "--host #{master.ip}",
          "--port #{master.port}",
          "--max-load Threads_running:#{limit_threads_running}",
          "--replicate #{schema}.checksums",
          "--replicate-database #{schema}",
          "--user #{username}",
          "--password #{password}"
        ].join ' '
        command_line += ' --resume' if previous_run
        
        # Spawn the process
        Open3.popen3(command_line) do |stdin, stdout, stderr, wait_thread|
          exit_code = nil
          pid = wait_thread.pid
          puts "Running pt-table-checksum targetting #{master}, pid on Jetpants host is #{pid}"
        
          self.collins_checksum_running = {
            'from_host' => Host.local.ip,
            'from_pid'  => pid,
            'timestamp' => Time.now.to_i,
          }.to_json
        
          # Display STDERR output in real-time, via a separate thread
          Thread.new do
            begin
              stderr.each {|line| puts line}
            rescue IOError, Interrupt
              nil
            end
          end
          
          # Capture STDOUT and buffer it; since this is the main thread, also
          # watch out for broken pipe or ctrl-c
          begin
            stdout.each {|line| output_lines << line}
            exit_code = wait_thread.value.to_i
          rescue IOError, Interrupt => ex
            puts "Caught exception #{ex.message}"
            exit_code = 130  # by unix convention, return 128 + SIGINT
          end
          
          # Dump out stdout: first anything we buffered on our end, plus anything
          # that Perl or the OS had buffered on its end
          puts
          output_lines.each {|line| puts line}
          unless stdout.eof?
            stdout.each {|line| puts line} rescue nil
          end
          puts
          
          puts "Checksum completed with exit code #{exit_code}.\n"
          success = (exit_code == 0)
          
          # Run again with --replicate-check-only to display ALL diffs, including ones from
          # prior runs of the tool.
          puts 'Verifying all results via --replicate-check-only...'
          output, diff_success = `#{command_line} --replicate-check-only`, $?.success?
          if diff_success
            puts 'No diffs found in any tables.'
            puts output
          else
            puts 'Found diffs:'
            puts output
            success = false
          end
          
          # Drop the checksums table, but only if there were no diffs
          if success
            output "Dropping table #{schema}.checksums..."
            master.connect(user: username, pass: password)
            master.query('DROP TABLE checksums')
            output "Table dropped."
            master.disconnect
            self.collins_checksum_running = ''
          else
            output 'Keeping checksums table in place for your review.'
            output 'Please manually drop it when done.'
          end
          puts
        end # popen3
      end # with_pt_checksum_user
      success
    end
    
    
    # Uses pt-upgrade to compare query performance and resultsets among nodes
    # in a pool. Supply params:
    # * a full path to a slowlog file
    # * a boolean indicating whether or not you want to do an initial silent
    #   run (results discarded) to populate the buffer pools on the nodes
    # * Two or more nodes, or no nodes if you want to default to using the
    #   pool's standby slaves
    #
    # Requires that pt-upgrade is in root's PATH on the node running Jetpants.
    def compare_queries(slowlog_path, silent_run_first, *compare_nodes)
      if compare_nodes.size == 0
        compare_nodes = standby_slaves
      else
        compare_nodes.flatten!
        raise "Supplied nodes must all be in this pool" unless compare_nodes.all? {|n| n == master || n.master == master}
      end
      
      # We need to create a temporary SUPER user on the nodes to compare
      # Also attempt to silence warning 1592 about unsafe-for-replication statements if
      # using Percona Server 5.5.10+ which supports this.
      username = 'pt-upgrade'
      password = DB.random_password
      remove_suppress_1592 = []
      compare_nodes.each do |node|
        node.create_user username, password
        node.grant_privileges username, '*', 'SUPER'
        node.grant_privileges username, node.app_schema, 'ALL PRIVILEGES'
        
        # We only want to try this if (a) the node supports log_warnings_suppress,
        # and (b) the node isn't already suppressing warning 1592
        if node.global_variables[:log_warnings_suppress] == ''
          node.mysql_root_cmd "SET GLOBAL log_warnings_suppress = '1592'"
          remove_suppress_1592 << node
        end
      end
      
      node_text = compare_nodes.map {|s| s.to_s + ' (v' + s.normalized_version(3) + ')'}.join ' vs '
      dsn_text = compare_nodes.map {|n| "h=#{n.ip},P=#{n.port},u=#{username},p=#{password},D=#{n.app_schema}"}.join ' '
      
      # Do silent run if requested (to populate buffer pools)
      if silent_run_first
        output "Doing a silent run of pt-upgrade with slowlog #{slowlog_path} to populate buffer pool."
        output "Comparing nodes #{node_text}..."
        stdout, exit_code = `pt-upgrade --set-vars wait_timeout=10000 #{slowlog_path} #{dsn_text} 2>&1`, $?.to_i
        output "pt-upgrade silent run completed with exit code #{exit_code}"
        puts
        puts
      end
      
      # Run pt-upgrade for real. Note that we only compare query times and results, NOT warnings,
      # due to issues with warning 1592 causing a huge amount of difficult-to-parse output.
      output "Running pt-upgrade with slowlog #{slowlog_path}"
      output "Comparing nodes #{node_text}..."
      stdout, exit_code = `pt-upgrade --set-vars wait_timeout=10000 --compare query_times,results #{slowlog_path} #{dsn_text} 2>&1`, $?.to_i
      output stdout
      puts
      output "pt-upgrade completed with exit code #{exit_code}"
      
      # Drop the SUPER user and re-enable logging of warning 1592
      compare_nodes.each {|node| node.drop_user username}
      remove_suppress_1592.each {|node| node.mysql_root_cmd "SET GLOBAL log_warnings_suppress = ''"}
    end
    
    
    # Collects query slowlog on the master (and one active slave, if there are any)
    # using tcpdump, copies over to the host Jetpants is running on, converts to a
    # slowlog, and then uses Pool#compare_queries to run pt-upgrade.
    #
    # The supplied *compare_nodes should be standby slaves, and you may omit them
    # to automatically select two standby slaves (of different versions, if available)
    # 
    # When comparing exactly two nodes, we stop replication on the nodes temporarily
    # to ensure a consistent dataset for comparing query results. Otherwise, async
    # replication can naturally result in false-positives.
    def collect_and_compare_queries!(tcpdump_time=30, *compare_nodes)
      # Sample traffic and convert to slowlog for master
      master_dump_filename = master.tcpdump!(tcpdump_time)
      local = Host.local # node where we're running Jetpants from
      local.ssh_cmd "mkdir -p #{Jetpants.export_location}"
      master.fast_copy_chain(Jetpants.export_location, local, files: master_dump_filename, overwrite: true)
      master.ssh_cmd "rm #{Jetpants.export_location}/#{master_dump_filename}"
      master_slowlog_path = local.dumpfile_to_slowlog("#{Jetpants.export_location}/#{master_dump_filename}")
      
      # If we also have an active slave running, grab sampled slowlog from there too
      active_slowlog_path = nil
      if active_slaves.size > 0
        active_slave = active_slaves.first
        active_dump_filename = active_slave.tcpdump!(tcpdump_time)
        active_slave.fast_copy_chain(Jetpants.export_location, local, files: active_dump_filename, overwrite: true)
        active_slave.ssh_cmd "rm #{Jetpants.export_location}/#{active_dump_filename}"
        active_slowlog_path = local.dumpfile_to_slowlog("#{Jetpants.export_location}/#{active_dump_filename}")
      end
      
      # Gather our comparison nodes
      if compare_nodes.size == 0
        higher_ver_standby = standby_slaves.select {|s| s.version_cmp(master) > 0}.first
        same_ver_standby = standby_slaves.select {|s| s.version_cmp(master) == 0}.first
        if higher_ver_standby && same_ver_standby
          compare_nodes = [same_ver_standby, higher_ver_standby]
        else
          compare_nodes = standby_slaves[0, 2]
        end
      end
      
      # Disable monitoring on our comparison nodes, and then stop replication
      # at the same position. We only proceed with this if we're comparing
      # exactly two nodes; this may be improved in a future release.
      if compare_nodes.size == 2
        compare_nodes.each {|n| n.disable_monitoring}
        compare_nodes.first.pause_replication_with(compare_nodes.last)
      end
      
      # Run pt-upgrade using the master dumpfile
      puts
      output "COMPARISON VIA QUERY LOG FROM MASTER"
      compare_queries(master_slowlog_path, true, *compare_nodes)
      
      if active_slowlog_path
        puts
        output "COMPARISON VIA QUERY LOG FROM ACTIVE SLAVE"
        compare_queries(active_slowlog_path, true, *compare_nodes)
      end
      
      # If we previously paused replication and disabled monitoring, un-do this
      if compare_nodes.size == 2
        compare_nodes.concurrent_each do |n| 
          n.resume_replication
          n.catch_up_to_master
          n.enable_monitoring
        end
      end
    end
    
  end
end
