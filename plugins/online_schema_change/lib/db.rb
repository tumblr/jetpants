module Jetpants
  class DB


    # make sure there is enough space to do an online schema change
    def has_space_for_alter?(table_name, database_name=nil)
      database_name ||= app_schema
      table_size = dir_size("#{mysql_directory}/#{database_name}/#{table_name}.ibd")

      table_size < mount_stats['available']
    end

    def drop_online_schema_change_triggers(database, table)
      database ||= app_schema
      ins_trigger = "pt_osc_#{database}_#{table}_ins"
      del_trigger = "pt_osc_#{database}_#{table}_del"
      upd_trigger = "pt_osc_#{database}_#{table}_upd"
      mysql_root_cmd("USE #{database}; DROP TRIGGER IF EXISTS #{ins_trigger}")
      mysql_root_cmd("USE #{database}; DROP TRIGGER IF EXISTS #{del_trigger}")
      mysql_root_cmd("USE #{database}; DROP TRIGGER IF EXISTS #{upd_trigger}")
    end
  end
end
