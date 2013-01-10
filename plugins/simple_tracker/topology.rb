module Jetpants
  class Topology
    
    attr_accessor :tracker
    
    ##### METHOD OVERRIDES #####################################################
    
    # Populates @pools by reading asset tracker data
    def load_pools
      @tracker = Jetpants::Plugin::SimpleTracker.new
      
      # Create Pool and Shard objects
      @pools.concat(@tracker.global_pools.map {|h| Pool.from_hash(h)}.compact)
      all_shards = @tracker.shards.map {|h| Shard.from_hash(h)}.reject {|s| s.state == :recycle}
      @pools.concat all_shards
      
      # Now that all shards exist, we can safely assign parent/child relationships
      @tracker.shards.each {|h| Shard.assign_relationships(h, all_shards)}
    end
    
    # Generates a database configuration file for a hypothetical web application
    def write_config
      config_file_path = @tracker.app_config_file_path
      
      # Convert the pool list into a hash
      db_data = {
        'database' => {
          'pools' => functional_partitions.map {|p| p.to_hash(true)},
          'shards' => shards.select {|s| s.in_config?}.map {|s| s.to_hash(true)},
        }
      }
      
      # Convert that hash to YAML and write it to a file
      File.open(config_file_path, 'w') do |f| 
        f.write db_data.to_yaml
      end
      puts "Regenerated #{config_file_path}"
    end
    
    # simple_tracker completely ignores any options like :role or :like
    def claim_spares(count, options={})
      raise "Not enough spare machines -- requested #{count}, only have #{@tracker.spares.count}" if @tracker.spares.count < count
      hashes = @tracker.spares.shift(count)
      update_tracker_data
      hashes.map {|h| h.is_a?(Hash) && h['node'] ? h['node'].to_db : h.to_db}
    end
    
    def count_spares(options={})
      @tracker.spares.count
    end
    
    
    ##### NEW METHODS ##########################################################
    
    # Called by Pool#sync_configuration to update our asset tracker json.
    # This actually re-writes all the json. With a more dynamic asset tracker
    # (something backed by a database, for example) this wouldn't be necessary -
    # instead Pool#sync_configuration could just update the info for that pool
    # only.
    def update_tracker_data
      @tracker.global_pools = functional_partitions.map &:to_hash
      @tracker.shards = shards.reject {|s| s.state == :recycle}.map &:to_hash
      @tracker.save
    end
    
    
  end
end
