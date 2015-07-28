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
    #   :ready          --  Normal shard, online / in production, optimal condition, no current operation/maintenance.
    #   :read_only      --  Online / in production but not currently writable due to maintenance or emergency.
    #   :offline        --  In production but not current readable or writable due to maintenance or emergency.
    #   :initializing   --  New child shard, being created, not in production.
    #   :exporting      --  Child shard that is exporting its portion of the data set. Shard not in production yet.
    #   :importing      --  Child shard that is importing its portion of the data set. Shard not in production yet.
    #   :replicating    --  Child shard that is being cloned to new replicas. Shard not in production yet.
    #   :child          --  In-production shard whose master is slaving from another shard. Reads go to to this shard's master, but writes go to the master of this shard's master and replicate down.
    #   :needs_cleanup  --  Child shard that is fully in production, but parent replication not torn down yet, and potentially has redundant data (from wrong range) not removed yet
    #   :deprecated     --  Parent shard that has been split but children are still in :child or :needs_cleanup state. Shard may still be in production for writes / replication not torn down yet.
    #   :recycle        --  Parent shard that has been split and children are now in the :ready state. Shard no longer in production, replication to children has been torn down.
    attr_accessor :state

    # the sharding pool to which this shard belongs
    attr_reader :shard_pool
    
    # Constructor for Shard --
    # * min_id: int
    # * max_id: int or the string "INFINITY"
    # * master: string (IP address) or a Jetpants::DB object
    # * state:  one of the above state symbols
    def initialize(min_id, max_id, master, state=:ready, shard_pool_name=nil)
      @min_id = min_id.to_i
      @max_id = (max_id.to_s.upcase == 'INFINITY' ? 'INFINITY' : max_id.to_i)
      @state = state

      @children = []    # array of shards being initialized by splitting this one
      @parent = nil
      shard_pool_name = Jetpants.topology.default_shard_pool if shard_pool_name.nil?
      @shard_pool = Jetpants.topology.shard_pool(shard_pool_name)
      
      super(generate_name, master)
    end
    
    # Generates a string containing the shard's min and max IDs. Plugin may want to override.
    def generate_name
      prefix = (@shard_pool.nil?) ? 'anon' : @shard_pool.name.downcase
      "#{prefix}-#{min_id}-#{max_id.to_s.downcase}"
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

    # Override the probe_tables method to accommodate shard topology -
    # delegate everything to the first shard.
    def probe_tables
      if Jetpants.topology.shards(self.shard_pool.name).first == self
        super
      else
        Jetpants.topology.shards(self.shard_pool.name).first.probe_tables
      end
    end

    # Override the tables accessor to accommodate shard topology - delegate
    # everything to the first shard
    def tables
      if Jetpants.topology.shards(self.shard_pool.name).first == self
        super
      else
        Jetpants.topology.shards(self.shard_pool.name).first.tables
      end
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
    
    # Splits a shard into <pieces> child shards.  The children will still be slaving
    # from the parent after this point; you need to do additional things to fully
    # complete the shard split.  See the command suite tasks shard_split_move_reads,
    # shard_split_move_writes, and shard_split_cleanup.
    #
    # You can optionally supply the ID ranges to use: pass in an array of arrays,
    # where the outer array is of size <pieces> and each inner array is [min_id, max_id].
    # If you omit id_ranges, the parent's ID range will be divided evenly amongst the
    # children automatically.
    def split!(pieces=2, id_ranges=false)
      raise "Cannot split a shard that is still a child!" if @parent
      raise "Cannot split a shard into #{pieces} pieces!" if pieces < 2
      
      # We can resume partially-failed shard splits if all children made it past
      # the :initializing stage. (note: some manual cleanup may be required first,
      # depending on where/how the split failed though.)
      num_children_post_init = @children.count {|c| c.state != :initializing}
      if (@children.size > 0 && @children.size != pieces) || (num_children_post_init > 0 && num_children_post_init != pieces)
        raise "Previous shard split died at an unrecoverable stage, cannot automatically restart"
      end
      
      # Set up the child shard masters, unless we're resuming a partially-failed
      # shard split
      if num_children_post_init == 0
        id_ranges ||= even_split_id_range(pieces)
        init_child_shard_masters(id_ranges)
      end
      
      shards_with_errors = []
      @children.concurrent_each do |c|
        c.prune_data! if [:initializing, :exporting, :importing].include? c.state
        begin
          c.clone_slaves_from_master
        rescue Exception => e
          shards_with_errors << {shard: c, error: e.message, stacktrace: e.backtrace.inspect}
        end
        c.sync_configuration
      end

      unless shards_with_errors.empty?
        shards_with_errors.each{|info| info[:shard].output info[:error]}
        raise "Error splitting shard #{self}."
      end
      
      output "Initial split complete."
    end

    # puts the shard in a state that triggers reads to move to child shards
    def move_reads_to_children
      @state = :deprecated
      
      @children.concurrent_each do |c|
        raise "Child shard #{c}  not in :replicating state!" if c.state != :replicating
      end

      @children.concurrent_each do |c|
        c.state = :child
        c.sync_configuration
      end
      sync_configuration
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
    
    # Exports data that should stay on this shard, drops and re-creates tables,
    # and then re-imports the data
    def prune_data!
      raise "Cannot prune a shard that isn't still slaving from another shard" unless @master.is_slave?
      unless [:initializing, :exporting, :importing].include? @state
        raise "Shard #{self} is not in a state compatible with calling prune_data! (current state=#{@state})"
      end
      
      tables = Table.from_config('sharded_tables', shard_pool.name)
      
      if @state == :initializing
        @state = :exporting
        sync_configuration
      end
      
      if @state == :exporting
        stop_query_killer
        export_schemata tables
        export_data tables, @min_id, @max_id
        @state = :importing
        sync_configuration
      end
      
      if @state == :importing
        stop_query_killer
        import_schemata!
        alter_schemata if respond_to? :alter_schemata
        disable_monitoring
        restart_mysql '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2', '--skip-slave-start'
        import_data tables, @min_id, @max_id
        restart_mysql # to clear out previous options '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2'
        enable_monitoring
        start_query_killer
      end
    end
    
    # Creates standby slaves for a shard by cloning the master.
    # Only call this on a child shard that isn't in production yet, or on
    # a production shard that's been marked as offline.
    def clone_slaves_from_master
      # If shard is already in state :child, it may already have slaves
      standby_slaves_needed  = slaves_layout[:standby_slave]
      standby_slaves_needed -= standby_slaves.size if @state == :child
      backup_slaves_needed  = slaves_layout[:backup_slave]
      backup_slaves_needed -= backup_slaves.size if @state == :child

      if standby_slaves_needed < 1 && backup_slaves_needed < 1
        output "Shard already has enough standby slaves and backup slaves, skipping step of cloning more"
        return
      end

      standby_slaves_available = Jetpants.topology.count_spares(role: :standby_slave, like: master)
      raise "Not enough standby_slave role machines in spare pool!" if standby_slaves_needed > standby_slaves_available

      backup_slaves_available = Jetpants.topology.count_spares(role: :backup_slave)
      if backup_slaves_needed > backup_slaves_available
        if standby_slaves_available > backup_slaves_needed + standby_slaves_needed &&
          agree("Not enough backup_slave role machines in spare pool, would you like to use standby_slaves? [yes/no]: ")

          standby_slaves_needed = standby_slaves_needed + backup_slaves_needed
          backup_slaves_needed = 0
        else
          raise "Not enough backup_slave role machines in spare pool!" if backup_slaves_needed > backup_slaves_available
        end
      end

      # Handle state transitions
      if @state == :child || @state == :importing
        @state = :replicating
        sync_configuration
      elsif @state == :offline || @state == :replicating
        # intentional no-op, no need to change state
      else
        raise "Shard #{self} is not in a state compatible with calling clone_slaves_from_master! (current state=#{@state})"
      end
      
      standby_slaves = Jetpants.topology.claim_spares(standby_slaves_needed, role: :standby_slave, like: master, for_pool: master.pool)
      backup_slaves = Jetpants.topology.claim_spares(backup_slaves_needed, role: :backup_slave, for_pool: master.pool)
      enslave!([standby_slaves, backup_slaves].flatten)
      [standby_slaves, backup_slaves].flatten.each &:resume_replication
      [self, standby_slaves, backup_slaves].flatten.each { |db| db.catch_up_to_master }
      
      @children
    end
    
    # Cleans up the state of a shard. This has two use-cases:
    # A. Run this on a parent shard after the rest of a shard split is complete.
    #    Sets this shard's master to read-only; removes the application user from
    #    self (without replicating this change to children); disables replication
    #    between the parent and the children; and then removes rows from the 
    #    children that replicated to the wrong shard.
    # B. Run this on a shard that just underwent a two-step promotion process which
    #    moved all reads, and then all writes, to a slave that has slaves of its own.
    #    For example, if upgrading MySQL on a shard by creating a newer-version slave
    #    and then adding slaves of its own to it (temp hierarchical replication setup).
    #    You can use this method to then "eject" the older-version master and its
    #    older-version slaves from the pool.
    def cleanup!
      raise "Cannot call cleanup! on a child shard" if @parent

      # situation A - clean up after a shard split
      if @state == :deprecated && @children.size > 0
        tables = Table.from_config('sharded_tables', pool.shard_pool.name)
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

      # situation B - clean up after a two-step (lockless) shard master promotion
      elsif @state == :needs_cleanup && @master.master && !@parent
        eject_master = @master.master
        eject_slaves = eject_master.slaves.reject { |s| s == @master } rescue []

        # stop the new master from replicating from the old master (we are about to eject)
        @master.disable_replication!

        eject_slaves.each(&:revoke_all_access!)
        eject_master.revoke_all_access!

        # We need to update the asset tracker to no longer consider the ejected
        # nodes as part of this pool. This includes ejecting the old master, which
        # might be handled by Pool#after_master_promotion! instead 
        # of Shard#sync_configuration.
        after_master_promotion!(@master, false) if respond_to? :after_master_promotion!
        
        @state = :ready

      else
        raise "Shard #{self} is not in a state compatible with calling cleanup! (state=#{state}, child count=#{@children.size}"
      end
      
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
    
    
    ###### Private methods #####################################################
    private
    
    # Splits self's ID range into num_children pieces
    # Returns an array of [low_id, high_id] arrays, suitable for
    # passing to Shard#init_child_shard_masters
    def even_split_id_range(num_children)
      raise "Cannot calculate an even split of last shard" if @max_id == 'INFINITY'
      id_ranges = []
      ids_total = 1 + @max_id - @min_id
      current_min_id = @min_id
      num_children.times do |i|
        ids_this_pool = (ids_total / num_children).floor
        ids_this_pool += 1 if i < (ids_total % num_children)
        id_ranges << [current_min_id, current_min_id + ids_this_pool - 1]
        current_min_id += ids_this_pool
      end
      id_ranges
    end
    
    # Early step of shard split process: initialize child shard pools, pull boxes from
    # spare list to use as masters for these new shards, and then populate them with the
    # full data set from self (the shard being split).
    #
    # Supply an array of [min_id, max_id] arrays, specifying the ID ranges to use for each
    # child. For example, if self has @min_id = 1001 and @max_id = 4000, and you're splitting
    # into 3 evenly-sized child shards, you'd supply [[1001,2000], [2001,3000], [3001, 4000]]
    def init_child_shard_masters(id_ranges)
      # Validations: make sure enough machines in spare pool; enough slaves of shard being split;
      # no existing children of shard being split
      # TODO: fix the first check to separately account for :role, ie check master and standby_slave counts separately
      # (this is actually quite difficult since we can't provide a :like node in a sane way)
      spares_needed = id_ranges.size * (1 + Jetpants.standby_slaves_per_pool)
      raise "Not enough machines in spare pool!" if spares_needed > Jetpants.topology.count_spares(role: :master, like: master)
      raise 'Shard split functionality requires Jetpants config setting "standby_slaves_per_pool" is at least 1' if Jetpants.standby_slaves_per_pool < 1
      raise "Must have at least #{Jetpants.standby_slaves_per_pool} slaves of shard being split" if master.slaves.size < Jetpants.standby_slaves_per_pool
      raise "Shard #{self} already has #{@children.size} child shards" if @children.size > 0
      
      # Set up the child shards, and give them masters
      id_ranges.each do |my_range|
        spare = Jetpants.topology.claim_spare(role: :master, like: master)
        spare.disable_read_only! if (spare.running? && spare.read_only?)
        spare.output "Will be master for new shard with ID range of #{my_range.first} to #{my_range.last} (inclusive)"
        child_shard = Shard.new(my_range.first, my_range.last, spare, :initializing, shard_pool.name)
        child_shard.sync_configuration
        add_child(child_shard)
        Jetpants.topology.add_pool child_shard
      end

      # We'll clone the full parent data set from a standby slave of the shard being split
      source = standby_slaves.first
      targets = @children.map &:master
      source.enslave_siblings! targets
    end
    
  end
end

