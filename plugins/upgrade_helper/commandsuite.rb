# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'upgrade_clone_slave', 'clone a standby slave to target node(s) running a newer version of MySQL'
    method_option :source, :desc => 'IP of node to clone from'
    method_option :target, :desc => 'IP of node(s) to clone to'
    def upgrade_clone_slave
      puts "This task clones the data set of a standby slave to target node(s) that have a"
      puts "newer version of MySQL already installed."
      source = ask_node('Please enter IP of node to clone from: ', options[:source])
      source.master.probe if source.master # fail early if there are any replication issues in this pool
      describe source
      
      puts "You may clone to particular IP address(es), or can type \"spare\" to claim a node from the spare pool."
      target = options[:target] || ask('Please enter comma-separated list of targets (IPs or "spare") to clone to: ')
      spares_needed = target.split(',').count {|t| t.strip.upcase == 'SPARE'}
      target = 'spare' if target.strip == '' || target.split(',').length == 0
      if spares_needed > 0
        spares_available = Jetpants.topology.count_spares(role: :standby_slave, like: source, version: Plugin::UpgradeHelper.new_version)
        raise "Not enough upgraded spares with role of standby slave! Requested #{spares_needed} but only have #{spares_available} available." if spares_needed > spares_available
        claimed_spares = Jetpants.topology.claim_spares(spares_needed, role: :standby_slave, like: source, version: Plugin::UpgradeHelper.new_version)
      end

      targets = target.split(',').map do |ip|
        ip.strip!
        if is_ip? ip
          ip.to_db
        elsif ip == '' || ip.upcase == 'SPARE'
          claimed_spares.shift
        else
          error "target (#{ip}) does not appear to be an IP."
        end
      end
      
      source.start_mysql if ! source.running?
      error "source (#{source}) is not a standby slave" unless source.is_standby?
      
      targets.each do |t|
        error "target #{t} already has a master; please clear out node (including in asset tracker) before proceeding" if t.master
      end
      
      # Disable fast shutdown on the source
      source.mysql_root_cmd 'SET GLOBAL innodb_fast_shutdown = 0'
      
      # Flag the nodes as needing upgrade, which will get triggered when
      # enslave_siblings restarts them
      targets.each {|t| t.needs_upgrade = true}
      
      # Remove ib_lru_dump if present on targets
      targets.concurrent_each {|t| t.ssh_cmd "rm -rf #{t.mysql_directory}/ib_lru_dump"}
      
      source.enslave_siblings!(targets)
      targets.concurrent_each {|t| t.resume_replication; t.catch_up_to_master}
      source.pool.sync_configuration
      
      puts "Clone-and-upgrade complete."
      Jetpants.topology.write_config
    end
    
    
    desc 'upgrade_promotion', 'demote and destroy a master running an older version of MySQL'
    method_option :demote,  :desc => 'node to demote'
    def upgrade_promotion
      demoted = ask_node 'Please enter the IP address of the node to demote:', options[:demote]
      demoted.probe
      
      # This task should not be used for emergency promotions (master failures)
      # since the regular "jetpants promotion" logic is actually fine in that case.
      error "Unable to connect to node #{demoted} to demote" unless demoted.running?
      
      # Before running this task, the pool should already have an extra standby slave,
      # since we're going to be removing the master from the pool.
      standby_slaves_needed = Jetpants.standby_slaves_per_pool + 1
      error "Only run this task on a pool with 3 standby slaves!" unless demoted.pool(true).standby_slaves.size >= standby_slaves_needed
      
      # Verify that all nodes except the master are running the same version, and
      # are higher version than the master
      unless demoted.slaves.all? {|s| s.version_cmp(demoted.slaves.first) == 0 && s.version_cmp(demoted) > 0}
        error "This task can only be used when all slaves are running the same version of MySQL,"
        error "and the master's version is older than that of all the slaves."
      end
      
      puts
      inform "Summary of affected pool"
      inform "Binary log positions and slave lag shown below are just a snapshot taken at the current time." if demoted.running?
      puts
      demoted.pool(true).summary(true)
      puts
      
      promoted = ask_node 'Please enter the IP address of a standby slave to promote: '
      
      error "Node to promote #{promoted} is not a standby slave of node to demote #{demoted}" unless promoted.master == demoted && promoted.role == :standby_slave
      error "The chosen node cannot be promoted. Please choose another." unless promoted.promotable_to_master?(false)
      
      inform "Going to DEMOTE AND DESTROY existing master #{demoted} and PROMOTE new master #{promoted}."
      error "Aborting." unless agree "Proceed? [yes/no]: "
      
      # Perform the promotion, but without making the old master become a slave of the new master
      # We then rely on the built-in call to Pool#sync_configuration or Pool#after_master_promotion!
      # to remove the old master from the pool in the same way it would handle a failed master (which
      # is entirely asset-tracker-plugin specific)
      demoted.pool(true).master_promotion!(promoted, false)
    end
    def self.after_upgrade_promotion
      reminders(
        'Commit/push the configuration in version control.',
        'Deploy the configuration to all machines.',
      )
    end
    
    
    desc 'shard_upgrade', 'upgrade a shard via four-step lockless process'
    method_option :min_id,  :desc => 'Minimum ID of shard to upgrade'
    method_option :max_id,  :desc => 'Maximum ID of shard to ugprade'
    method_option :reads,   :desc => 'Move reads to the new master', :type => :boolean
    method_option :writes,  :desc => 'Move writes to new master', :type => :boolean
    method_option :cleanup, :desc => 'Tear down the old-version nodes', :type => :boolean
    def shard_upgrade
      if options[:reads]
        raise 'The --reads, --writes, and --cleanup options are mutually exclusive' if options[:writes] || options[:cleanup]
        s = ask_shard_being_upgraded :reads
        s.branched_upgrade_move_reads
        Jetpants.topology.write_config
        self.class.reminders(
          'Commit/push the configuration in version control.',
          'Deploy the configuration to all machines.',
          'Wait for reads to stop on the old shard master.',
          'Proceed to next step: jetpants shard_upgrade --writes'
        )
      elsif options[:writes]
        raise 'The --reads, --writes, and --cleanup options are mutually exclusive' if options[:reads] || options[:cleanup]
        s = ask_shard_being_upgraded :writes
        s.branched_upgrade_move_writes
        Jetpants.topology.write_config
        self.class.reminders(
          'Commit/push the configuration in version control.',
          'Deploy the configuration to all machines.',
          'Wait for writes to stop on the old parent master.',
          'Proceed to next step: jetpants shard_upgrade --cleanup',
        )
        
      elsif options[:cleanup]
        raise 'The --reads, --writes, and --cleanup options are mutually exclusive' if options[:reads] || options[:writes]
        s = ask_shard_being_upgraded :cleanup
        s.cleanup!
        
      else
        self.class.reminders(
          'This process may take an hour or two. You probably want to run this from a screen session.',
          'Be especially careful if you are relying on SSH Agent Forwarding for your root key, since this is not screen-friendly.'
        )
        s = ask_shard_being_upgraded :prep
        s.branched_upgrade_prep
        self.class.reminders(
          'Proceed to next step: jetpants shard_upgrade --reads'
        )
      end
    end
    
    
    desc 'checksum_pool', 'Run pt-table-checksum on a pool to verify data consistency after an upgrade of one slave'
    method_option :pool,  :desc => 'name of pool'
    def checksum_pool
      pool_name = options[:pool] || ask('Please enter name of pool to checksum: ')
      pool = Jetpants.topology.pool(pool_name) or raise "Pool #{pool_name} does not exist"
      pool.checksum_tables
    end
    
    
    desc 'check_pool_queries', 'Runs pt-upgrade on a pool to verify query performance and results between different MySQL versions'
    method_option :pool, :desc => 'name of pool'
    method_option :dumptime, :desc => 'number of seconds of tcpdump data to consider'
    def check_pool_queries
      pool_name = options[:pool] || ask('Please enter name of pool to checksum: ')
      dump_time = options[:dumptime].to_i if options[:dumptime]
      dump_time ||= 30
      
      pool = Jetpants.topology.pool(pool_name) or raise "Pool #{pool_name} does not exist"
      pool.collect_and_compare_queries!(dump_time)
    end
    
    no_tasks do
      def ask_shard_being_upgraded(stage=:prep)
        shards_being_upgraded = Jetpants.shards.select {|s| [:child, :needs_cleanup].include?(s.state) && !s.parent && s.master.master}
        if stage == :writes || stage == :cleanup
          if shards_being_upgraded.size == 0
            raise 'No shards are currently being upgraded. You can only use this task after running "jetpants shard_upgrade".'
          elsif shards_being_upgraded.size == 1
            s = shards_being_upgraded.first
            puts "Detected #{s} as the only shard currently involved in an upgrade operation."
            error "Aborting." unless agree "Is this the right shard that you want to perform this action on? [yes/no]: "
            return s
          else
            puts "The following shards are already involved in an upgrade operation:"
            shards_being_upgraded.each {|sbu| puts "* #{sbu}"}
          end
        end
        puts "Which shard would you like to perform this action on?"
        shard_min = options[:min_id] || ask('Please enter min ID of the shard: ')
        shard_max = options[:max_id] || ask('Please enter max ID of the shard: ')
        s = Jetpants.topology.shard shard_min, shard_max
        raise 'Shard not found' unless s
        s
      end
    end
    
  end
end