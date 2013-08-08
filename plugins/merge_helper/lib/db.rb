module Jetpants
  class DB
    def aggregator?
      return @aggregator unless @aggregator.nil?
      version_info = query_return_array('SHOW VARIABLES LIKE "version"')
      @aggregator = !version_info.nil? && !version_info.empty? && version_info.first[:Value].downcase.include?("mariadb")
    end

    def ship_schema_to(node)
      export_schemata tables
      fast_copy_chain(
        Jetpants.export_location,
        node,
        port: 3307,
        files: [ "create_tables_#{@node.port}.sql" ]
      )
    end

  end
end
