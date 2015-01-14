module Jetpants
  module Plugin
    module MergeHelper
      class << self
        # Provide a config hook to specify a list of tables to merge, overriding the sharded_tables list
        def tables_to_merge
          tables = Table.from_config 'sharded_tables'
          table_list = []
          if (!Jetpants.plugins['merge_helper'].nil? && Jetpants.plugins['merge_helper'].has_key?('table_list'))
            table_list = Jetpants.plugins['merge_helper']['table_list']
          end
          tables.select! { |table| table_list.include? table.name } unless table_list.empty?
          tables
        end

        def perform_duplicate_check_if_necessary
          unless Jetpants.plugins['merge_helper']['min_id_dup_check'].nil? ||
                 Jetpants.plugins['merge_helper']['max_id_dup_check'].nil? ||
                 Jetpants.plugins['merge_helper']['table_dup_check'].nil?
            duplicates_found = Shard.identify_merge_duplicates(shards_to_merge,
                                                               Jetpants.plugins['merge_helper']['min_id_dup_check'],
                                                               Jetpants.plugins['merge_helper']['max_id_dup_check'],
                                                               Jetpants.plugins['merge_helper']['table_dup_check'])
            raise "Fix the duplicates manually before proceeding for the merge" unless duplicates_found == false
          end
        end
      end
    end
  end
end

# load all the monkeypatches for other Jetpants classes
%w(db shard table commandsuite aggregator).each {|mod| require "merge_helper/lib/#{mod}"}
