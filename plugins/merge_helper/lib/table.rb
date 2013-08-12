module Jetpants

  class Table
    include CallbackHandler

    # Generate a query to determine if there are any rows outside of the shard id range
    def sql_range_check(sharding_key, min_id, max_id)
      sql = "SELECT count(*) AS invalid_records FROM #{@name} WHERE #{sharding_key} > #{max_id} OR #{sharding_key} < #{min_id}"

      return sql
    end

    def export_filenames(min_id, max_id)

    end
  end
end
