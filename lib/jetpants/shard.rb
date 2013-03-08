require 'json'
require 'db'
require 'table'
require 'pool'


module Jetpants
  
  # a Shard in Jetpants is a range-based Pool.  All Shards have the exact same
  # set of tables, just they only contain data that falls within within their
  # range.
  class Shard < Pool
    include CallbackHandler

    # min ID for this shard
    attr_reader :min_id
    
    # max ID for this shard, or string "INFINITY"
    attr_reader :max_id
    
    # if this shard is being split, this is an array of "child" Shard objects.
    attr_reader :children
    
    # if this shard is a child of one being split, this links back to the parent Shard.
    attr_accessor :parent
    
    # A symbol representing the shard's state. Possible state values include:
    #   :ready          --  Normal shard, online / in production, optimal codition, no current operation/maintenance.
    #   :read_only      --  Online / in production but not currently writable due to maintenance or emergency.
    #   :offline        --  In production but not current readable or writable due to maintenance or emergency.
    #   :initializing   --  New child shard, being created, not in production.
    #   :exporting      --  Child shard that is exporting its portion of the data set. Shard not in production yet.
    #   :importing      --  Child shard that is importing its portion of the data set. Shard not in production yet.
    #   :replicating    --  Child shard that is being cloned to new replicas. Shard not in production yet.
    #   :child          --  Child shard that is in production for reads, but still slaving from its parent for writes.
    #   :needs_cleanup  --  Child shard that is fully in production, but parent replication not torn down yet, and redundant data (from wrong range) not removed yet
    #   :deprecated     --  Parent shard that has been split but children are still in :child or :needs_cleanup state. Shard may still be in production for writes / replication not torn down yet.
    #   :recycle        --  Parent shard that has been split and children are now in the :ready state. Shard no longer in production, replication to children has been torn down.
    attr_accessor :state
    
    # Constructor for Shard --
    # * min_id: int
    # * max_id: int or the string "INFINITY"
    # * master: string (IP address) or a Jetpants::DB object
    # * state:  one of the above state symbols
    def initialize(min_id, max_id, master, state=:ready)
      @min_id = min_id.to_i
      @max_id = (max_id.to_s.upcase == 'INFINITY' ? 'INFINITY' : max_id.to_i)
      @state = state

      @children = []    # array of shards being initialized by splitting this one
      @parent = nil
      
      super(generate_name, master)
    end
    
    # Generates a string containing the shard's min and max IDs. Plugin may want to override.
    def generate_name
      "shard-#{min_id}-#{max_id.to_s.downcase}"
    end
    
    # Returns true if the shard state is one of the values that indicates it's
    # a live / in-production shard. These states include :ready, :child,
    # :needs_cleanup, :read_only, and :offline.
    def in_config?
      [:ready, :child, :needs_cleanup, :read_only, :offline].include? @state
    end
    
    # In default Jetpants, we assume each Shard has 1 master and N standby slaves;
    # we never have active (read) slaves for shards. So calling mark_slave_active
    # on a Shard generates an exception. Plugins may override this behavior, which
    # may be necessary for sites spanning two or more active data centers.
    def mark_slave_active(slave_db, weight=100)
      raise "Shards do not support active slaves"
    end
    
    # Returns an empty array, because we assume that shard pools have no active
    # slaves. (If your read volume would require active slaves, think about
    # splitting your shard instead...)
    #
    # Plugins may of course override this behavior.
    def active_slaves
     []
    end
    
    # Returns the master's standby slaves, ignoring any child shards since they
    # are a special case of slaves.
    def standby_slaves
      result = super
      if @children.count > 0
        is_child_master = {}
        @children.each {|c| is_child_master[c.master] = true}
        result.reject {|sl| is_child_master[sl]}
      else
        result
      end
    end
    
    # Returns the Jetpants::DB object corresponding to the requested access
    # mode (either :read or :write).  Ordinarily this will be the shard's
    # @master, unless this shard is still a child, in which case we send
    # writes the the shard's parent's master instead.
    def db(mode=:read)
      (mode.to_sym == :write && @parent ? @parent.master : master)
    end
    
    # Adds a Jetpants::Shard to this shard's array of children, and sets
    # the child's parent to be self.
    def add_child(shard)
      raise "Shard #{shard} already has a parent!" if shard.parent
      @children << shard
      shard.parent = self
    end
    
    # Removes a Jetpants::Shard from this shard's array of children, and sets
    # the child's parent to nil.
    def remove_child(shard)
      raise "Shard #{shard} isn't a child of this shard!" unless shard.parent == self
      @children.delete shard
      shard.parent = nil
    end
    
    # Creates and returns <count> child shards, pulling boxes for masters from spare list.
    # You can optionally supply the ID ranges to use: pass in an array of arrays,
    # where the outer array is of size <count> and each inner array is [min_id, max_id].
    # If you omit id_ranges, the parent's ID range will be divided evenly amongst the
    # children automatically.
    def init_children(count, id_ranges=false)
      # Make sure we have enough machines (of correct hardware spec and role) in spare pool
      raise "Not enough master role machines in spare pool!" if count > Jetpants.topology.count_spares(role: :master, like: master)
      raise "Not enough standby_slave role machines in spare pool!" if count * Jetpants.standby_slaves_per_pool > Jetpants.topology.count_spares(role: :standby_slave, like: slaves.first)
      
      # Make sure enough slaves of shard being split
      raise "Must have at least #{Jetpants.standby_slaves_per_pool} slaves of shard being split" if master.slaves.count < Jetpants.standby_slaves_per_pool
      
      # Make sure right number of id_ranges were supplied, if any were
      raise "Wrong number of id_ranges supplied" if id_ranges && id_ranges.count != count
      
      unless id_ranges
        id_ranges = []
        ids_total = 1 + @max_id - @min_id
        current_min_id = @min_id
        count.times do |i|
          ids_this_pool = (ids_total / count).floor
          ids_this_pool += 1 if i < (ids_total % count)
          id_ranges << [current_min_id, current_min_id + ids_this_pool - 1]
          current_min_id += ids_this_pool
        end
      end
      
      count.times do |i|
        spare = Jetpants.topology.claim_spare(role: :master, like: master)
        spare.disable_read_only! if (spare.running? && spare.read_only?)
        spare.output "Using ID range of #{id_ranges[i][0]} to #{id_ranges[i][1]} (inclusive)"
        s = Shard.new(id_ranges[i][0], id_ranges[i][1], spare, :initializing)
        add_child(s)
        Jetpants.topology.pools << s
        s.sync_configuration
      end
      
      @children
    end
    
    # Splits a shard into <pieces> child shards.  The children will still be slaving
    # from the parent after this point; you need to do additional things to fully
    # complete the shard split.  See the command suite tasks shard_split_move_reads,
    # shard_split_move_writes, and shard_split_cleanup.
    def split!(pieces=2)
      raise "Cannot split a shard that is still a child!" if @parent
      
      init_children(pieces) unless @children.count > 0
      
      clone_to_children!
      @children.concurrent_each {|c| c.rebuild!}
      @children.each {|c| c.sync_configuration}
      
      @state = :deprecated
      sync_configuration
      output "Initial split complete."
    end
    
    # Transitions the shard's children into the :needs_cleanup state. It is the
    # responsibility of an asset tracker plugin / config generator to implement
    # config generation in a way that actually makes writes go to shards
    # in the :needs_cleanup state.
    def move_writes_to_children
      @children.each do |c| 
        c.state = :needs_cleanup
        c.sync_configuration
      end
    end
    
    # Clones the current shard to its children.  Uses a standby slave of self as
    # the source for copying.
    def clone_to_children!
      # Figure out which slave(s) we can use for populating the new masters
      sources = standby_slaves.dup
      raise "Need to have at least 1 slave in order to create additional slaves" if sources.length < 1
      
      # If we have 2 or more slaves, keep 1 replicating for safety's sake; don't use it for spinning up children
      sources.shift if sources.length > 1
      
      # Figure out which machines we need to turn into slaves
      targets = []
      @children.each do |child_shard|
        if child_shard.master.is_slave? && child_shard.master.master != @master
          raise "Child shard master #{child_shard.master} is already a slave of another pool"
        elsif child_shard.master.is_slave?
          child_shard.output "Already slaving from parent shard master"
        else
          targets << child_shard.master
        end
      end
      
      while targets.count > 0 do
        chain_length = (targets.count.to_f / sources.count.to_f).ceil
        chain_length = 3 if chain_length > 3 # For sanity's sake, we only allow a copy pipeline that populates 3 instances at once.
        sources.concurrent_each_with_index do |src, idx|
          my_targets = targets[idx * chain_length, chain_length]
          src.enslave_siblings! my_targets
          chain_length.times {|n| targets[(idx * chain_length) + n] = nil}
        end
        targets.compact!
      end
    end
    
    # Exports data that should stay on this shard, drops and re-creates tables,
    # re-imports the data, and then adds slaves to the shard pool as needed.
    def rebuild!
      # Sanity check
      raise "Cannot rebuild a shard that isn't still slaving from another shard" unless @master.is_slave?
      raise "Cannot rebuild an active shard" if in_config?
      
      tables = Table.from_config 'sharded_tables'
      
      if [:initializing, :exporting].include? @state
        @state = :exporting
        sync_configuration
        stop_query_killer
        export_schemata tables
        export_data tables, @min_id, @max_id
      end
      
      if [:exporting, :importing].include? @state
        @state = :importing
        sync_configuration
        import_schemata!
        alter_schemata if respond_to? :alter_schemata
        restart_mysql '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2', '--skip-slave-start'
        import_data tables, @min_id, @max_id
        restart_mysql # to clear out previous options '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2'
        start_query_killer
      end
      
      if [:importing, :replicating].include? @state
        @state = :replicating
        sync_configuration
        if Jetpants.standby_slaves_per_pool > 0
          my_slaves = Jetpants.topology.claim_spares(Jetpants.standby_slaves_per_pool, role: :standby_slave, like: parent.slaves.first)
          enslave!(my_slaves)
          my_slaves.each {|slv| slv.resume_replication}
          [self, my_slaves].flatten.each {|db| db.catch_up_to_master}
        else
          catch_up_to_master
        end
      else
        raise "Shard not in a state compatible with calling rebuild! (current state=#{@state})"
      end
      
      @state = :child
    end
    
    # Run this on a parent shard after the rest of a shard split is complete.
    # Sets this shard's master to read-only; removes the application user from
    # self (without replicating this change to children); disables replication
    # between the parent and the children; and then removes rows from the 
    # children that replicated to the wrong shard.
    def cleanup!
      raise "Can only run cleanup! on a parent shard in the deprecated state" unless @state == :deprecated
      raise "Cannot call cleanup! on a child shard" if @parent
      
      tables = Table.from_config 'sharded_tables'
      @master.revoke_all_access!
      @children.concurrent_each do |child_shard|
        raise "Child state does not indicate cleanup is needed" unless child_shard.state == :needs_cleanup
        raise "Child shard master should be a slave in order to clean up" unless child_shard.is_slave?
        child_shard.master.disable_replication! # stop slaving from parent
        child_shard.prune_data_to_range tables, child_shard.min_id, child_shard.max_id
      end
      
      # We have to iterate over a copy of the @children array, rather than the array
      # directly, since Array#each skips elements when you remove elements in-place,
      # which Shard#remove_child does...
      @children.dup.each do |child_shard|
        child_shard.state = :ready
        remove_child child_shard
        child_shard.sync_configuration
      end
      @state = :recycle
      sync_configuration
    end
    
    # Displays information about the shard
    def summary(extended_info=false, with_children=false)
      super(extended_info)
      if with_children
        children.each {|c| c.summary}
      end
      true
    end
    
  end
end

