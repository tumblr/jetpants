module Jetpants

  class Table
    include CallbackHandler

    # Generate a query to determine if there are any rows outside of the shard id range
    def sql_range_check(sharding_key, min_id, max_id)
      "SELECT count(*) AS invalid_records FROM #{@name} WHERE #{sharding_key} > #{max_id} OR #{sharding_key} < #{min_id}"
    end

    # Generate a list of chunked filenames for import/export
    def export_filenames(min_id, max_id)
      export_filenames = []
      (min_id..max_id).in_chunks(@chunks) do |min, max|
        export_filenames << export_file_path(min, max)
      end

      export_filenames
    end
  end
end
