module Jetpants
  class DB

    # Creates a temporary user for use of pt-table-checksum and pt-upgrade,
    # yields to the supplied block, and then drops the user.
    # The user will have a randomly-generated 50-character password, and will
    # have elevated permissions (ALL PRIVILEGES on the application schema, and
    # a few global privs as well) since these are necessary to run the tools.
    # The block will be passed the randomly-generated password.
    def with_online_schema_change_user(username, database)
      password = DB.random_password
      create_user username, password
      grant_privileges username, '*', 'PROCESS', 'REPLICATION CLIENT', 'REPLICATION SLAVE', 'SUPER'
      grant_privileges username, database, 'ALL PRIVILEGES'
      begin
        yield password
      rescue StandardError, Interrupt, IOError
        drop_user username
        raise
      end
      drop_user username
    end

    # make sure there is enough space to do an online schema change
    def has_space_for_alter?(table_name, database_name=nil)
      database_name ||= app_schema
      table_size = dir_size("#{mysql_directory}/#{database_name}/#{table_name}.ibd")

      table_size < mount_stats['available']
    end

  end
end
