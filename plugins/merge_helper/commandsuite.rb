# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'merge_shards', 'Merge two or more shards using an aggregator instance'
    def merge_shards
      shards_to_merge = []
      aggregate_node
      spares_for_aggregate_shard = Jetpants.topology.claim_spares(Jetpants.standby_slaves_per_pool + 1, role: :standby_slave, like: shards_to_merge.first.master)
      # We need to pass in a master here, the aggregator instance?
      aggregate_shard = new Shard(shards_to_merge.first.min_id, shards_to_merge.last.max_id, nil, :initializing)

      # is this necessary?
      shards_to_merge.each do |shard|
        shard.children ||= [ aggregate_shard ]
      end

      # need to export schema somewhere

      total_export_counts = {}
      total_import_counts = {}
      slaves_to_replicate = []

      tables = Table.from_config 'sharded_tables'
      shards_to_merge.each do |shard|
        slave = shard.standby_slaves.last
        slave.pause_replication
        slaves_to_replicate << slave
        export_counts = slave.export_data tables

        if total_export_counts.empty?
          total_export_counts = export_counts
        else
          total_export_counts.keys.each do |key|
            total_export_counts[key] = total_export_counts[key] + export_counts[key]
          end
        end

        files = tables.map { |table| table.export_file_path.basename }
        fast_copy_chain(
          Jetpants.export_location,
          aggregate_node,
          port: 3306,
          files: files,
          overwrite: true
        )

        import_counts = aggregate_node.import_data tables
        if total_import_counts.empty?
          total_import_counts = import_counts
        else
          total_import_counts.keys.each do |key|
            total_import_counts[key] = total_import_counts[key] + import_counts[key]
          end
        end
      end

      raise "Imported and exported table count doesn't match!" unless total_import_counts.keys.count == total_export_counts.keys.count
      valid = true;
      total_import_count.each do |key, val|
        if val != total_export_count[key]
          output "Count for export/import of #{key} is wrong! (#{val} imported #{total_export_count[key]} exported)"
          valid = false
        end
      end

      raise "Import/export counts do not match, aborting" unless valid

      aggregate_node.add_nodes_to_aggregate slaves_to_replicate
      slaves_to_replicate.concurrent_each do |slave|
        slave.resume_replication
        slave.catch_up_to_master
      end

      aggregate_node.start_all_slaves

      sync_configuration
    end
  end
end

