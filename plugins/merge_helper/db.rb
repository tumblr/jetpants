module Jetpants
  class DB
    def initialize(ip, port=3306)
      raise "Attempting to initialize a database without aggregation capabilities as an aggregate node" unless aggregate?

      super
    end

    def aggregator?
      return @aggregator unless @aggregator.nil?
      version_info = query_return_array('SHOW VARIABLES LIKE "%version%"')
      @aggregator = !version_info[:version_comment].nil? && version_info[:version_comment].downcase.include? "mariadb"
    end

  end
end
