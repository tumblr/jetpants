# Additions to the thor command suite
# New commands for initially registering shards, pools, and spares

require 'thor'

module Jetpants
  class CommandSuite < Thor
    desc 'add_pool', 'inform the asset tracker about a pool that was not previously tracked'
    method_option :name,  :desc => 'name of pool'
    method_option :master, :desc => 'IP address of pool master'
    def add_pool
      pool_name = options[:name] || ask('Please enter the name of the pool to add: ')
      raise "Name is required" unless pool_name && pool_name.length > 0
      node = ask_node("Please enter the IP of the pool's master: ", options[:master])
      p = Pool.new(pool_name, node)
      p.sync_configuration
      Jetpants.topology.write_config
      puts 'Be sure to manually register any active read slaves using "jetpants activate_slave"' if p.slaves.count > 0
    end
    
    desc 'add_shard', 'inform the asset tracker about a shard that was not previously tracked'
    method_option :min_id, :desc => 'Minimum ID of shard to track'
    method_option :max_id, :desc => 'Maximum ID of shard to track'
    method_option :master, :desc => 'IP address of shard master'
    def add_shard
      min_id = options[:min_id] || ask('Please enter min ID of the shard: ')
      max_id = options[:max_id] || ask('Please enter max ID of the shard: ')
      min_id = min_id.to_i
      max_id = (max_id.to_s.upcase == 'INFINITY' ? 'INFINITY' : max_id.to_i)
      node = ask_node("Please enter the IP of the pool's master: ", options[:master])
      s = Shard.new(min_id, max_id, node)
      s.sync_configuration
      Jetpants.topology.write_config
    end
    
    desc 'add_spare', 'register a spare node with the asset tracker'
    method_option :node, :desc => 'Clean-state node to register as spare -- should be previously untracked'
    def add_spare
      node = ask_node("Please enter the IP of the spare node: ", options[:node])
      
    end
  end
end
