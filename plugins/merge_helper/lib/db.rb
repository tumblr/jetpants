module Jetpants
  class DB
    def aggregator?
      return @aggregator unless @aggregator.nil?
      version_info = query_return_array('SHOW VARIABLES LIKE "%version%"')
      @aggregator = !version_info[:version_comment].nil? && version_info[:version_comment].downcase.include?("mariadb")
    end

  end
end
