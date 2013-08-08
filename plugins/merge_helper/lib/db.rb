module Jetpants
  class DB
    def aggregator?
      return @aggregator unless @aggregator.nil?
      version_info = query_return_array('SHOW VARIABLES LIKE "version"')
      @aggregator = !version_info.nil? && !version_info.empty? && version_info.first[:Value].downcase.include?("mariadb")
    end

  end
end
