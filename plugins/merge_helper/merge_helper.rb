module Jetpants
  module Plugin
    module MergeHelper
      class << self
        # Provide a config hook to specify a list of tables to merge, overriding the sharded_tables list
        def tables_to_merge(shard_pool)
          tables = Table.from_config('sharded_tables', shard_pool)
          table_list = []
          if (!Jetpants.plugins['merge_helper'].nil? && Jetpants.plugins['merge_helper'].has_key?('table_list'))
            table_list = Jetpants.plugins['merge_helper']['table_list']
          end
          tables.select! { |table| table_list.include? table.name } unless table_list.empty?
          tables
        end
      end
    end
  end
end

# load all the monkeypatches for other Jetpants classes
%w(db shard table commandsuite aggregator).each {|mod| require "merge_helper/lib/#{mod}"}
