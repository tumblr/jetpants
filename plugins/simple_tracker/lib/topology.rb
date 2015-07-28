module Jetpants
  class Topology
    
    def self.tracker
      @tracker ||= Jetpants::Plugin::SimpleTracker.new

      @tracker
    end
    
    ##### METHOD OVERRIDES #####################################################
    
    # Populates @pools by reading asset tracker data
    def load_pools

      # Create Pool and Shard objects
      @pools = self.class.tracker.global_pools.map {|h| Pool.from_hash(h)}.compact
      all_shards = self.class.tracker.shards.map {|h| Shard.from_hash(h)}.reject {|s| s.state == :recycle}
      @pools.concat all_shards

      # Now that all shards exist, we can safely assign parent/child relationships
      self.class.tracker.shards.each {|h| Shard.assign_relationships(h, all_shards)}
    end

    # Populate @shard_pools by reading asset tracker data
    def load_shard_pools
      @shard_pools = self.class.tracker.shard_pools.map{|h| ShardPool.from_hash(h) }.compact
    end

    def add_pool(pool)
      @pools << pool unless pools.include? pool
    end

    def add_shard_pool(shard_pool)
      @shard_pools << shard_pool unless shard_pools.include? shard_pool
    end

    # Generates a database configuration file for a hypothetical web application
    def write_config
      config_file_path = self.class.tracker.app_config_file_path

      # Convert the pool list into a hash
      db_data = {
        'database' => {
          'pools' => functional_partitions.map {|p| p.to_hash(true)},
        },
        'shard_pools' => {}
      }

      shard_pools.each do |shard_pool|
        db_data['shard_pools'][shard_pool.name] = shard_pool.shards.select {|s| s.in_config?}.map {|s| s.to_hash(true)}
      end

      # Convert that hash to YAML and write it to a file
      File.open(config_file_path, 'w') do |f|
        f.write db_data.to_yaml
        f.close
      end
      puts "Regenerated #{config_file_path}"
    end

    # simple_tracker completely ignores any options like :role or :like
    def claim_spares(count, options={})
      raise "Not enough spare machines -- requested #{count}, only have #{self.class.tracker.spares.count}" if self.class.tracker.spares.count < count
      hashes = self.class.tracker.spares.shift(count)
      update_tracker_data
      dbs = hashes.map {|h| h.is_a?(Hash) && h['node'] ? h['node'].to_db : h.to_db}

      if options[:for_pool]
        pool = options[:for_pool]
        dbs.each do |db|
          pool.claimed_nodes << db unless pool.claimed_nodes.include? db
        end
      end
       
      dbs
    end
    
    def count_spares(options={})
      self.class.tracker.spares.count
    end

    def spares(options={})
      self.class.tracker.spares.map(&:to_db)
    end

    
    ##### NEW METHODS ##########################################################
    
    # Called by Pool#sync_configuration to update our asset tracker json.
    # This actually re-writes all the json. With a more dynamic asset tracker
    # (something backed by a database, for example) this wouldn't be necessary -
    # instead Pool#sync_configuration could just update the info for that pool
    # only.
    def update_tracker_data
      self.class.tracker.global_pools = functional_partitions.map &:to_hash
      self.class.tracker.shards = pools.select{|p| p.is_a? Shard}.reject {|s| s.state == :recycle}.map &:to_hash
      self.class.tracker.shard_pools = shard_pools.map(&:to_hash)
      self.class.tracker.save
    end

  end
end
