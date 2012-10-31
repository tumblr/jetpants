# JetCollins monkeypatches to add Collins integration

module Jetpants
  class Host
    
    ##### JETCOLLINS MIX-IN ####################################################
    
    include Plugin::JetCollins
    
    def collins_asset
      # try IP first; failing that, try hostname
      selector = {ip_address: ip, details: true}
      selector[:remoteLookup] = true if Jetpants.plugins['jetpants_collins']['remote_lookup']
      assets = Plugin::JetCollins.find selector

      if (!assets || assets.count == 0) && available?
        selector = {hostname: "^#{hostname}$", details: true}
        selector[:remoteLookup] = true if Jetpants.plugins['jetpants_collins']['remote_lookup']
        assets = Plugin::JetCollins.find selector
      end
      
      raise "Multiple assets found for #{self}" if assets.count > 1
      if ! assets || assets.count == 0
        output "WARNING: no Collins assets found for this host"
        nil
      else
        assets.first
      end
    end
    
    # Returns which datacenter this host is in. Only a getter, intentionally no setter.
    def collins_location
      return @collins_location if @collins_location
      ca = collins_asset
      @collins_location ||= (ca ? ca.location || Plugin::JetCollins.datacenter : 'unknown')
      @collins_location.upcase!
      @collins_location
    end
    
  end
end