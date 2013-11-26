module Jetpants
  class DB
    # Tests against the version of mysql that's running to determine if it is an aggregator node
    # Override this in a custom plugin to accurately determine whether a node is an aggregator or not
    def aggregator?
      false
    end

    # Export and ship the table schema to a specified node
    # WARNING! The export created will be destructive to any data on the destination node!
    def ship_schema_to(node)
      export_schemata tables
      fast_copy_chain(
        Jetpants.export_location,
        node,
        port: 3307,
        files: [ "create_tables_#{node.port}.sql" ],
        overwrite: true
      )
    end

    # Provide external access to import/export counts
    def import_export_counts
      @counts
    end

    # all the insertion of combined export counts for validation
    def inject_counts(counts)
      @counts = counts
    end
  end
end
