# JetCollins monkeypatches to add Collins integration

require 'yaml'
require 'json'

module Jetpants
  class Topology

    ##### METHODS THAT OTHER PLUGINS CAN OVERRIDE ##############################
    
    # IMPORTANT NOTE
    # This plugin does NOT implement write_config, since this format of
    # your app configuration file entirely depends on your web framework!
    #
    # You will have to implement this yourself in a separate plugin; recommended
    # approach is to add serialization methods to Pool and Shard, and call it
    # on each @pool, writing out to a file or pinging a config service, depending
    # on whatever your application uses.
    
    
    # Handles extra options for querying spare nodes. Takes a Collins selector
    # hash and an options hash, and returns a potentially-modified Collins
    # selector hash.
    # The default implementation here implements no special logic. Custom plugins
    # (loaded AFTER jetpants_collins is loaded) can override this method to
    # manipulate the selector; see commented-out example below. 
    def process_spare_selector_options(selector, options)
      # If you wanted to support an option of :role, and map this to the Collins
      # SECONDARY_ROLE attribute, you could implement this via:
      #    selector[:secondary_role] = options[:role].to_s.downcase if options[:role]
      # This could be useful if, for example, you use a different hardware spec
      # for masters vs slaves. (doing so isn't really recommended, which is why
      # we omit this logic by default.)
      
      # return the selector
      selector
    end
    
    ##### METHOD OVERRIDES #####################################################

    def load_shard_pools
      @shard_pools = configuration_assets('MYSQL_SHARD_POOL').map(&:to_shard_pool)
      @shard_pools.compact!
      @shard_pools.sort_by! { |p| p.name }

      true
    end

    # Initializes list of pools + shards from Collins
    def load_pools
      load_shard_pools if @shard_pools.nil?

      # We keep a cache of Collins::Asset objects, organized as pool_name => role => [asset, asset, ...]
      @pool_role_assets = {}

      # Populate the cache for all master and active_slave nodes. (We restrict to these types
      # because this is sufficient for creating Pool objects and generating app config files.)
      Jetpants.topology.server_node_assets(false, :master, :active_slave)

      @pools = configuration_assets('MYSQL_POOL', 'MYSQL_SHARD').map(&:to_pool)
      @pools.compact! # remove nils from pools that had no master
      @pools.sort_by! { |p| sort_pools_callback p }

      # Set up parent/child relationships between shards currently being split.
      # We do this in a separate step afterwards so that Topology#pool can find the parent
      # by name, regardless of ordering in Collins
      @pools.select {|p| p.has_parent}.each do |p|
        parent = pool(p.has_parent) or raise "Cannot find parent shard named #{p.has_parent}"
        parent.add_child(p)
      end
      true
    end

    def add_pool(pool)
      raise 'Attempt to add a non pool to the pools topology' unless pool.is_a?(Pool)

      unless pools.include? pool
        @pools << pool
        @pools.sort_by! { |p| sort_pools_callback p }
      end
      true
    end

    def add_shard_pool(shard_pool)
      raise 'Attempt to add a non shard pool to the sharding pools topology' unless shard_pool.is_a?(ShardPool)

      unless shard_pools.include? shard_pool
        @shard_pools << shard_pool
        @shard_pools.sort_by! { |sp| sp.name }
      end
    end

    # Returns (count) DB objects.  Pulls from machines in the spare state
    # and converts them to the Allocated status.
    # You can pass in :role to request spares with a particular secondary_role
    def claim_spares(count, options={})
      return [] if count == 0
      assets = query_spare_assets(count, options)
      raise "Not enough spare machines available! Found #{assets.count}, needed #{count}" if assets.count < count
      claimed_dbs = assets.map do |asset|
        db = asset.to_db
        db.claim!
        if options[:for_pool]
          options[:for_pool].claimed_nodes << db unless options[:for_pool].claimed_nodes.include? db
        end

        db
      end

      if options[:for_pool]
        compare_pool = options[:for_pool]
      elsif options[:like] && options[:like].pool
        compare_pool = options[:like].pool
      else
        compare_pool = false
      end

      if(compare_pool && claimed_dbs.select{|db| db.proximity_score(compare_pool) > 0}.count > 0)
        compare_pool.output "Unable to claim #{count} nodes with an ideal proximity score!" 
      end

      claimed_dbs
    end

    # This method won't ever return a number higher than 100, but that's
    # not a problem, since no single operation requires that many spares
    def count_spares(options={})
      query_spare_assets(100, options).count
    end

    # This method won't ever return more than than 100 nodes, but that's
    # not a problem, since no single operation requires that many spares
    def spares(options={})
      query_spare_assets(100, options).map(&:to_db)
    end

    ##### NEW METHODS ##########################################################

    def db_location_report(shards_only = nil)
      unless shards_only.nil?
        pools_to_consider = shards(shards_only)
      else
        pools_to_consider = pools
      end

      global_map = {}
      pools_to_consider.reduce(global_map){ |map, shard| map.deep_merge!(shard.db_layout,Hash::DEEP_MERGE_CONCAT) }
      global_map
    end

    # Returns an array of Collins::Asset objects meeting the given criteria.
    # Caches the result for subsequent use.
    # Optionally supply a pool name to restrict the result to that pool.
    # Optionally supply one or more role symbols (:master, :active_slave,
    # :standby_slave, :backup_slave) to filter the result to just those
    # SECONDARY_ROLE values in Collins.
    def server_node_assets(pool_name=false, *roles)
      roles = normalize_roles(roles) if roles.count > 0
      
      # Check for previously-cached result. (Only usable if a pool_name supplied.)
      if pool_name && @pool_role_assets[pool_name]
        if roles.count > 0 && roles.all? {|r| @pool_role_assets[pool_name].has_key? r}
          return roles.map {|r| @pool_role_assets[pool_name][r]}.flatten
        elsif roles.count == 0 && valid_roles.all? {|r| @pool_role_assets[pool_name].has_key? r}
          return @pool_role_assets[pool_name].values.flatten
        end
      end
      
      per_page = Jetpants.plugins['jetpants_collins']['selector_page_size'] || 50
      selector = {
        operation:    'and',
        details:      true,
        size:         per_page,
        query:        'primary_role = ^DATABASE$ AND type = ^SERVER_NODE$ AND status != ^DECOMMISSIONED$'
      }
      selector[:remoteLookup] = true if Jetpants.plugins['jetpants_collins']['remote_lookup']
      selector[:query] += " AND pool = ^#{pool_name}$" if pool_name
      if roles.count == 1
        selector[:query] += " AND secondary_role = ^#{roles.first}$"
      elsif roles.count > 1
        values = roles.map {|r| "secondary_role = ^#{r}$"}
        selector[:query] += ' AND (' + values.join(' OR ') + ')'
      end
      
      assets = []
      done = false
      page = 0

      # Query Collins one or more times, until we've seen all the results
      until done do
        selector[:page] = page
        # find() apparently alters the selector object now, so we dup it
        # also force JetCollins to retry requests to the Collins server
        results = Plugin::JetCollins.find selector.dup, true, page == 0
        done = (results.count < per_page) || (results.count == 0 && page > 0) 
        page += 1
        assets.concat(results.select {|a| a.pool}) # filter out any spare nodes, which will have no pool set
      end

      # Next we need to update our @pool_role_assets cache. But first let's set it to [] for each pool/role
      # that we queried. This intentionally nukes any previous cached data, and also allows us to differentiate
      # between an empty result and a cache miss.
      roles = valid_roles if roles.count == 0
      seen_pools = assets.map {|a| a.pool.downcase}
      seen_pools << pool_name if pool_name
      seen_pools.uniq.each do |p|
        @pool_role_assets[p] ||= {}
        roles.each {|r| @pool_role_assets[p][r] = []}
      end
      
      # Filter
      assets.select! {|a| a.pool && a.secondary_role && %w(allocated maintenance).include?(a.status.downcase)}
      
      # Cache
      assets.each {|a| @pool_role_assets[a.pool.downcase][a.secondary_role.downcase.to_sym] << a}
      
      # Return
      assets
    end
    
    
    # Returns an array of configuration assets with the supplied primary role(s)
    def configuration_assets(*primary_roles)
      raise "Must supply at least one primary_role" if primary_roles.count < 1
      per_page = Jetpants.plugins['jetpants_collins']['selector_page_size'] || 50
      
      selector = {
        operation:    'and',
        details:      true,
        size:         per_page,
        query:        'status != ^DECOMMISSIONED$ AND type = ^CONFIGURATION$',
      }

      if primary_roles.count == 1
        selector[:primary_role] = primary_roles.first
      else
        values = primary_roles.map {|r| "primary_role = ^#{r}$"}
        selector[:query] += ' AND (' + values.join(' OR ') + ')'
      end
      
      selector[:remoteLookup] = true if Jetpants.plugins['jetpants_collins']['remote_lookup']

      done = false
      page = 0
      assets = []
      until done do
        selector[:page] = page
        # find() apparently alters the selector object now, so we dup it
        # also force JetCollins to retry requests to the Collins server
        page_of_results = Plugin::JetCollins.find selector.dup, true, page == 0
        assets += page_of_results
        page += 1
        done = (page_of_results.count < per_page) || (page_of_results.count == 0 && page > 0)
      end
      
      # If remote lookup is enabled, remove the remote copy of any pool that exists
      # in both local and remote datacenters.
      if Jetpants.plugins['jetpants_collins']['remote_lookup']
        dc_pool_map = {Plugin::JetCollins.datacenter => {}}
        
        assets.each do |a|
          location = a.location || Plugin::JetCollins.datacenter
          pool = a.pool ? a.pool.downcase : a.tag[6..-1].downcase  # if no pool, strip 'mysql-' off front and use that
          dc_pool_map[location] ||= {}
          dc_pool_map[location][pool] = a
        end
        
        # Grab everything from current DC first (higher priority over other
        # datacenters), then grab everything from remote DCs.
        final_assets = dc_pool_map[Plugin::JetCollins.datacenter].values
        dc_pool_map.each do |dc, pool_to_assets|
          next if dc == Plugin::JetCollins.datacenter
          pool_to_assets.each do |pool, a|
            final_assets << a unless dc_pool_map[Plugin::JetCollins.datacenter][pool]
          end
        end
        assets = final_assets
      end

      assets
    end
    
    
    def clear_asset_cache(pool_name=false)
      if pool_name
        @pool_role_assets.delete pool_name
      else
        @pool_role_assets = {}
      end
    end
    
    
    private
    
    # Helper method to query Collins for spare DBs.
    def query_spare_assets(count, options={})
      per_page = Jetpants.plugins['jetpants_collins']['selector_page_size'] || 50

      # Intentionally no remoteLookup=true here.  We only want to grab spare nodes
      # from the datacenter that Jetpants is running in.
      selector = {
        operation:        'and',
        details:          true,
        type:             'SERVER_NODE',
        status:           'Allocated',
        state:            'SPARE',
        primary_role:     'DATABASE',
        size:             per_page,
      }
      selector = process_spare_selector_options(selector, options)
      source = options[:like]

      done = false
      page = 0
      nodes = []
      until done do
        selector[:page] = page
        # find() apparently alters the selector object now, so we dup it
        # also force JetCollins to retry requests to the Collins server
        page_of_results = Plugin::JetCollins.find selector.dup, true, page == 0
        nodes += page_of_results
        done = (page_of_results.count < per_page) || (page_of_results.count == 0 && page > 0)
        page += 1
      end
      
      keep_assets = []
      
      nodes.map(&:to_db).concurrent_each {|db| db.probe rescue nil}
      nodes.concurrent_each do |node|
        db = node.to_db
        if(db.usable_spare? &&
          (
            !source ||
            (!source.pool && db.usable_with?(source)) ||
            (
              (!options[:for_pool] && source.pool && db.usable_in?(source.pool)) ||
              (options[:for_pool] && db.usable_in?(options[:for_pool]))
            )
          )
        )
          keep_assets << node
        end
      end

      if options[:for_pool]
        compare_pool = options[:for_pool]
      elsif source && source.pool
        compare_pool = source.pool
      else
        compare_pool = false
      end

      # here we compare nodes against the optionally provided source to attempt to
      # claim a node which is not physically local to the source nodes
      if compare_pool
        keep_assets = sort_assets_for_pool(compare_pool, keep_assets)
      end

      keep_assets.slice(0,count)
    end

    def sort_assets_for_pool(pool, assets)
      assets.sort! do |lhs, rhs|
        lhs.to_db.proximity_score(pool) <=> rhs.to_db.proximity_score(pool)
      end

      assets
    end

    def sort_pools_callback(pool)
      asset = pool.collins_asset
      role = asset.primary_role.upcase
      shard_pool_name = ''

      case role
        when 'MYSQL_POOL'
          position = (asset.config_sort_order || 0).to_i
        when 'MYSQL_SHARD'
          position = asset.shard_min_id.to_i
          shard_pool_name = pool.shard_pool.name
        else
          position = 0
      end

      [role, shard_pool_name, position]
    end

  end
end
