module Jetpants
  
  # Topology maintains a list of all DB pools/shards, and is responsible for
  # reading/writing configurations and manages spare box assignments.
  # Much of this behavior needs to be overridden by a plugin to actually be
  # useful.  The implementation here is just a stub.
  class Topology
    include CallbackHandler
    include Output

    def initialize
      # initialize @pools to an empty state
      @pools  = nil

      # initialize shard pools to empty
      @shard_pools = nil

      # We intentionally don't call load_pools here. The caller must do that.
      # This allows Jetpants module to create Jetpants.topology object, and THEN
      # invoke load_pools, which might then refer back to Jetpants.topology.
    end

    def to_s
      "Jetpants.topology"
    end

    def pools
      load_pools if @pools.nil?
      @pools
    end

    def shard_pools
      load_shard_pools if @shard_pools.nil?
      @shard_pools
    end

    def default_shard_pool
      raise "Default shard pool not defined!" if Jetpants.default_shard_pool.nil?
      Jetpants.default_shard_pool
    end

    ###### Class methods #######################################################
    
    # Metaprogramming hackery to create a "synchronized" method decorator
    # Note that all synchronized methods share the same mutex, so don't make one
    # synchronized method call another!
    @lock = Mutex.new
    @do_sync = false
    @synchronized_methods = {} # symbol => true
    class << self
      # Decorator that causes the next method to be wrapped in a mutex
      # (only affects the next method definition, not ALL subsequent method
      # definitions)
      # If the method is subsequently overridden by a plugin, the new version
      # will be synchronized as well, even if the decorator is omitted.
      def synchronized
        @do_sync = true
      end
      
      def method_added(name)
        if @do_sync || @synchronized_methods[name]
          lock = @lock
          @do_sync = false
          @synchronized_methods[name] = false # prevent infinite recursion from the following line
          alias_method "#{name}_without_synchronization".to_sym, name
          define_method name do |*args|
            result = nil
            lock.synchronize {result = send "#{name}_without_synchronization".to_sym, *args}
            result
          end
          @synchronized_methods[name] = true # remember it is synchronized, to re-apply wrapper if method overridden by a plugin
        end
      end
    end
    
    
    ###### Overridable methods ################################################
    # Plugins should override these if the behavior is needed. (Note that plugins
    # don't need to repeat the "synchronized" decorator; it automatically
    # applies to overrides.)

    synchronized
    # Plugin should override so that this reads in a configuration and initializes
    # @pools as appropriate.
    def load_pools
      output "Notice: no plugin has overridden Topology#load_pools, so *no* pools are imported automatically"
    end

    synchronized
    # Plugin should override this to initialize @shard_pools
    def load_shard_pools
      output "Notice: no plugin has overridden Topology#load_shard_pools, so *no* shard pools are imported automaticaly"
    end

    synchronized
    # Plugin should override so that this adds the given pool to the current topology (@pools)
    def add_pool(pool)
      output "Notice: no plugin has overridden Topology#add_pool, so the pool was *not* added to the topology"
    end

    synchronized
    # Plugin should override so that this adds the given shard pool to the current topology (@shard_pools)
    def add_shard_pool(shard_pool)
      output "Notice: no plugin has overridden Topology#add_shard_pool, so the shard pool was *not* added to the topology"
    end

    synchronized
    # Plugin should override so that it writes a configuration file or commits a
    # configuration change to a config service.
    def write_config
      output "Notice: no plugin has overridden Topology#write_config, so configuration data is *not* saved"
    end
    
    synchronized
    # Plugin should override so that this returns an array of [count] Jetpants::DB
    # objects, or throws an exception if not enough left.
    #
    # Options hash is plugin-specific. Jetpants core will provide these two options,
    # but it's up to a plugin to handle (or ignore) them:
    #
    # :role   =>  :master or :standby_slave, indicating what purpose the new node(s)
    #             will be used for. Useful if your hardware spec varies by node role
    #             (not recommended!) or if you vet your master candidates more carefully.
    # :like   =>  a Jetpants::DB object, indicating that the spare node hardware spec
    #             should be like the specified DB's spec.
    def claim_spares(count, options={})
      raise "Plugin must override Topology#claim_spares"
    end
    
    synchronized
    # Plugin should override so that this returns a count of spare machines
    # matching the selected options. options hash follows same format as for
    # Topology#claim_spares.
    def count_spares(options={})
      raise "Plugin must override Topology#count_spares"
    end

    synchronized
    # Plugin should override so that this returns a list of spare machines
    # matching the selected options. options hash follows same format as for
    # Topology#claim_spares.
    def spares(options={})
      raise "Plugin must override Topology#spares"
    end

    # Returns a list of valid role symbols in use in Jetpants.
    def valid_roles
      [:master, :active_slave, :standby_slave, :backup_slave]
    end
    
    # Returns a list of valid role symbols which indicate a slave status
    def slave_roles
      valid_roles.reject {|r| r == :master}
    end
    
    ###### Instance Methods ####################################################
    
    # Returns array of this topology's Jetpants::Pool objects of type Jetpants::Shard
    def shards(shard_pool_name = nil)
      if shard_pool_name.nil?
        shard_pool_name = default_shard_pool 
        output "Using default shard pool #{default_shard_pool}"
      end
      pools.select {|p| p.is_a? Shard}.select { |p| p.shard_pool && p.shard_pool.name.downcase == shard_pool_name.downcase }
    end
    
    # Returns array of this topology's Jetpants::Pool objects that are NOT of type Jetpants::Shard
    def functional_partitions
      pools.reject {|p| p.is_a? Shard}
    end
    
    # Finds and returns a single Jetpants::Pool. Target may be a name (string, case insensitive)
    # or master (DB object).
    def pool(target)
      if target.is_a?(DB)
        pools.select {|p| p.master == target}.first
      else
        pools.select {|p| p.name.downcase == target.downcase}.first
      end
    end
    
    # Finds and returns a single Jetpants::Shard
    def shard(min_id, max_id, shard_pool_name = nil)
      shard_pool_name = default_shard_pool if shard_pool_name.nil?
      if max_id.is_a?(String) && max_id.upcase == 'INFINITY'
        max_id.upcase!
      else
        max_id = max_id.to_i
      end

      min_id = min_id.to_i

      shards(shard_pool_name).select {|s| s.min_id == min_id && s.max_id == max_id}.first
    end

    # Finds a ShardPool object by name
    def shard_pool(name)
      shard_pools.select{|sp| sp.name.downcase == name.downcase}.first
    end
    
    # Returns the Jetpants::Shard that handles the given ID.
    # During a shard split, if the child isn't "in production" yet (ie, it's
    # still being built), this will always return the parent shard. Once the
    # child is fully built / in production, this method will always return
    # the child shard. However, Shard#db(:write) will correctly delegate writes
    # to the parent shard when appropriate in this case. (see also: Topology#shard_db_for_id)
    def shard_for_id(id, shard_pool = nil)
      shard_pool = default_shard_pool if shard_pool.nil?
      choices = shards(shard_pool).select {|s| s.min_id <= id && (s.max_id == 'INFINITY' || s.max_id >= id)}
      choices.reject! {|s| s.parent && ! s.in_config?} # filter out child shards that are still being built
      
      # Preferentially return child shards at this point
      if choices.any? {|s| s.parent}
        choices.select {|s| s.parent}.first
      else
        choices.first
      end
    end
    
    # Returns the Jetpants::DB that handles the given ID with the specified
    # mode (either :read or :write)
    def shard_db_for_id(id, mode=:read, shard_pool = nil)
      shard_for_id(id, shard_pool).db(mode)
    end
    
    # Nicer inteface into claim_spares when only one DB is desired -- returns
    # a single Jetpants::DB object instead of an array.
    def claim_spare(options={})
      claim_spares(1, options)[0]
    end
    
    # Returns if the supplied role is valid
    def valid_role? role
      valid_roles.include? role.to_s.downcase.to_sym
    end
    
    # Converts the supplied roles (strings or symbols) into lowercase symbol versions
    # Will expand out special role of :slave to be all slave roles.
    def normalize_roles(*roles)
      roles = roles.flatten.map {|r| r.to_s.downcase == 'slave' ? slave_roles.map(&:to_s) : r.to_s.downcase}.flatten
      roles.each {|r| raise "#{r} is not a valid role" unless valid_role? r}
      roles.uniq.map &:to_sym
    end
    
    synchronized
    # Clears the pool list and nukes cached DB and Host object lookup tables
    def clear
      @pools = nil
      @shard_pools = nil
      DB.clear
      Host.clear
    end
    
    # Empties and then reloads the pool list
    def refresh
      clear
      load_shard_pools
      load_pools
      true
    end
  end
end
