# Entrypoint for simple_tracker example asset tracker plugin.
# config options:
#   tracker_data_file_path  --  path and filename of where to save the asset data JSON file
#   app_config_file_path    --  path and filename of where to save the database configuration YAML file for a fictional web app

require 'json'

module Jetpants
  module Plugin
    
    # The SimpleTracker class just handles the manipulations of the asset JSON file and the application
    # YAML file. The Jetpants::Topology class is monkeypatched to maintain a single SimpleTracker object,
    # which it uses to interact with these files.
    class SimpleTracker
      # Array of hashes, each containing info from Pool#to_hash
      attr_accessor :global_pools
      
      # Array of hashes, each containing info from Shard#to_hash
      attr_accessor :shards

      # Array of hashes, each containing info from ShardPool#to_hash
      attr_accessor :shard_pools
      
      # Clean state DB nodes that are ready for use. Array of any of the following:
      # * hashes each containing key 'node'. could expand to include 'role' or other metadata as well,
      #   but currently not supported.
      # * objects responding to to_db, such as String or Jetpants::DB
      attr_accessor :spares
      
      attr_reader :app_config_file_path
      
      def initialize
        @tracker_data_file_path = Jetpants.plugins['simple_tracker']['tracker_data_file_path'] || '/etc/jetpants_tracker.json'
        @app_config_file_path   = Jetpants.plugins['simple_tracker']['app_config_file_path']   || '/var/lib/mysite/config/databases.yaml'
        data = JSON.parse(File.read(@tracker_data_file_path)) rescue {'pools' => {}, 'shards' => [], 'spares' => [], 'shard_pools' => []}
        @global_pools = data['pools']
        @shards = data['shards']
        @spares = data['spares']
        @shard_pools = data['shard_pools']
      end
      
      def save
        File.open(@tracker_data_file_path, 'w') do |f|
          data = {'pools' => @global_pools, 'shards' => @shards, 'spares' => @spares, 'shard_pools' => @shard_pools}
          f.puts JSON.pretty_generate(data)
          f.close
        end
      end

      def determine_pool_and_role(ip, port=3306)
        ip += ":#{port}"
        (@global_pools + @shards).each do |h|
          pool = (h['name'] ? Jetpants.topology.pool(h['name']) : Jetpants.topology.shard(h['min_id'], h['max_id']))
          return [pool, 'MASTER'] if h['master'] == ip
          h['slaves'].each do |s|
            return [pool, s['role']] if s['host'] == ip
          end
        end

        raise "Unable to find #{ip} among tracked assets"
      end
      
      def determine_slaves(ip, port=3306)
        ip += ":#{port}"
        
        (@global_pools + @shards).each do |h|
          next unless h['master'] == ip
          return h['slaves'].map {|s| s['host'].to_db}
        end
        [] # return empty array if not a master
      end
      
    end
  end
end

# load all the monkeypatches for other Jetpants classes
%w(pool shard topology db shardpool commandsuite).each { |mod| require "simple_tracker/lib/#{mod}" }
