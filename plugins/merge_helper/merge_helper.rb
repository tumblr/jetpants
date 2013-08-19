module Jetpants
  module Plugin
    module MergeHelper
      class << self
        # Provide a config hook to specify a list of tables to merge, overriding the sharded_tables list
        def tables_to_merge
          tables = Table.from_config 'sharded_tables' unless tables
          table_list = Jetpants.plugins['merge_helper']['table_list']
          tables.select! { |table| table_list.include? table.name } if table_list
          tables
        end
      end
    end
  end
end

# load all the monkeypatches for other Jetpants classes
%w(db shard table commandsuite aggregator).each {|mod| require "merge_helper/lib/#{mod}"}
