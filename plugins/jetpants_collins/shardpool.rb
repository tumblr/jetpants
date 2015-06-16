module Jetpants
  # A ShardPool is a sharding keyspace in Jetpants that contains
  # many Shards.  All shards within the pool partition a logically coherent
  # keyspace

  class ShardPool

    ##### JETCOLLINS MIX-IN ####################################################

    include Plugin::JetCollins

    collins_attr_accessor :shard_pool

    def collins_asset(create_if_missing=false)
      selector = {
        operation:    'and',
        details:      true,
        type:         'CONFIGURATION',
        primary_role: '^MYSQL_SHARD_POOL$',
        shard_pool:   "^#{@name.upcase}$",
        status:       'Allocated',
      }
      selector[:remoteLookup] = true if Jetpants.plugins['jetpants_collins']['remote_lookup']

      results = Plugin::JetCollins.find selector, !create_if_missing

      # If we got back multiple results, try ignoring the remote datacenter ones
      if results.count > 1
        filtered_results = results.select {|a| a.location.nil? || a.location.upcase == Plugin::JetCollins.datacenter}
        results = filtered_results if filtered_results.count > 0
      end

      if results.count > 1
        raise "Multiple configuration assets found for pool #{@name}"
      elsif results.count == 0 && create_if_missing
        output "Could not find configuration asset for pool; creating now"
        new_tag = 'mysql-shard-pool-' + @name
        asset = Collins::Asset.new type: 'CONFIGURATION', tag: new_tag, status: 'Allocated'
        begin
          Plugin::JetCollins.create!(asset)
        rescue
          collins_set asset:  asset,
                      status: 'Allocated'
        end
        collins_set asset: asset,
                    primary_role: 'MYSQL_SHARD_POOL',
                    shard_pool: @name.upcase
        Plugin::JetCollins.get new_tag
      elsif results.count == 0 && !create_if_missing
        raise "Could not find configuration asset for pool #{name}"
      else
        results.first
      end
    end
  end
end
