# JetCollins monkeypatches to add Collins integration

module Jetpants
  class DB

    ##### JETCOLLINS MIX-IN ####################################################

    include Plugin::JetCollins

    def before_enslave_siblings!(targets)
      targets.select(&:is_spare?).each(&:claim!)
    end
  end
end
