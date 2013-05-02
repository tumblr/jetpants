module Jetpants
  class Shard
    # Builds a set of upgraded slaves, and then makes one of the new slaves become the
    # master for the other new slaves
    def branched_upgrade_prep
      raise "Shard #{self} in wrong state to perform this action! expected :ready, found #{@state}" unless @state == :ready
      raise "Not enough standby slaves of this shard!" unless standby_slaves.size >= Jetpants.standby_slaves_per_pool
      source = standby_slaves.last
      spares_available = Jetpants.topology.count_spares(role: :standby_slave, like: source, version: Plugin::UpgradeHelper.new_version)
      raise "Not enough spares available!" unless spares_available >= 1 + Jetpants.standby_slaves_per_pool
      
      targets = Jetpants.topology.claim_spares(1 + Jetpants.standby_slaves_per_pool, role: :standby_slave, like: source, version: Plugin::UpgradeHelper.new_version)
      
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
      
      # Make the 1st new slave be the "future master" which the other new
      # slaves will replicate from
      future_master = targets.shift
      targets.each do |t|
        future_master.pause_replication_with t
        t.change_master_to future_master
        [future_master, t].each {|db| db.resume_replication; db.catch_up_to_master}
      end
    end
    
    # Hack the pool configuration to send reads to the new master, but still send
    # writes to the old master (they'll replicate over)
    def branched_upgrade_move_reads
      raise "Shard #{self} in wrong state to perform this action! expected :ready, found #{@state}" unless @state == :ready
      future_master = nil
      slaves.each do |s|
        future_master = s if s.version_cmp(@master) == 1 && s.slaves.size == Jetpants.standby_slaves_per_pool
      end
      raise "Shard #{self} does not have correct hierarchical replication setup to proceed" unless future_master
      @master = future_master
      @state = :child
      sync_configuration
    end
    
    # Move writes over to the new master
    def branched_upgrade_move_writes
      raise "Shard #{self} in wrong state to perform this action! expected :child, found #{@state}" unless @state == :child
      @master.disable_read_only!
      @state = :needs_cleanup
      sync_configuration
    end
    
  end
end
