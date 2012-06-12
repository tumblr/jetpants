module Jetpants
  module Tasks
    class Promotion

      def initialize nodes = {}
        @demoted  = nodes['demote']
        @promoted = nodes['promote']
        super
        Jetpants.verify_replication = false # since master may be offline
        establish_roles
        execute_promotion
      end

      def error message
        abort ['ERROR:'.red, message].join ' '
      end

      def inform message
        puts message.blue
      end
    
      def is_ip? address
        address =~ /(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/
      end

      def establish_roles
        establish_demoted
        establish_replicas
        establish_promoted
      end

      def establish_demoted
        # derive demoted from promoted if possible
        if @promoted and not @demoted
          error "invalid ip address #{@promoted}" unless is_ip? @promoted
          @promoted = Jetpants::DB.new @promoted

          # bail the promoted node isn't a slave or we can't connect
          unless @promoted.is_slave?
            error "node (#{@promoted}) does not appear to be a replica of another node"
          end rescue error("unable to connect to node #{@promoted} to promote")

          # recommend a node to demote
          agreed = agree [
            "Would you like to demote the following node?",
            "address: #{@promoted.master}",
            "slaves : #{@promoted.master.slaves.join(', ')}",
            "- yes/no -"
          ].join "\n"
          error "unable to promote #{@promoted} unless you demote #{@promoted.master}" unless agreed

          @demoted = @promoted.master.ip
        end
      
        # unable to derive demoted, so ask and convert to a DB object
        unless @demoted.kind_of? Jetpants::DB
          @demoted = ask 'Please enter the node to demote:' unless @demoted
          error "Invalid IP address #{@demoted}" unless is_ip? @demoted
          @demoted = @demoted.to_db
        end
          
        # connect and ensure node is a master; handle offline nodes appropriately
        if @demoted.available?
          error 'Cannot demote a node that has no slaves!' unless @demoted.has_slaves?
        else
          inform "unable to connect to node #{@demoted} to demote"
          error  "unable to perform promotion" unless agree "please confirm that #{@demoted} is offline: yes/no "
          @replicas = @demoted.slaves # An asset-tracker plugin may have been populated the slave list anyway
          if !@replicas || @replicas.count < 1
            replicas = ask "please provide a comma seperated list of current replicas of #{@demoted}: ", lambda {|replicas| replicas.split /,\s*/}
            error "user supplied list of replicas appears to be invalid - #{replicas}" unless replicas.all? {|replica| is_ip? replica}
            replicas = replicas.map &:to_db
            @demoted.instance_eval {@slaves = replicas}
            @replicas = replicas
            
            # ensure they were replicas of @demoted
            @replicas.each do |replica|
              error "#{replica} does not appear to be a valid replica of #{@demoted}" unless replica.master == @demoted
            end
          end
        end
          
        error 'unable to establish demoteable node' unless @demoted.kind_of? Jetpants::DB
      end

      def establish_replicas
        @replicas ||= @demoted.slaves
        error 'no replicas to promote' if @replicas.empty?
        error 'replicas appear to be invalid' unless @replicas.all? {|replica| replica.kind_of? Jetpants::DB}
        inform "#{@demoted} has the following replicas: #{@replicas.join(', ')}"
      end

      def establish_promoted
        # user supplied node to promote
        if @promoted and not @promoted.kind_of? Jetpants::DB
          error "invalid ip address #{@promoted}" unless is_ip? @promoted
          @promoted = Jetpants::DB.new @promoted
        end

        # user hasn't supplied a valid node to promote
        unless @replicas.include? @promoted
          inform "unable to promote node (#{@promoted}) that is not a replica of #{@demoted}" if @promoted

          # recommend a node
          puts "\nREPLICA LIST:"
          @replicas.sort_by {|replica| replica.seconds_behind_master}.each do |node|
            file, pos = node.repl_binlog_coordinates(false)
            puts " * %-13s %-30s  lag: %2ds   coordinates: (%-13s, %d)" % [node.ip, node.hostname, node.seconds_behind_master, file, pos]
          end
          puts
          recommended = @replicas.sort_by {|replica| replica.seconds_behind_master}.reject {|r| r.for_backups?}.first
          agreed = agree [
            "Would you like to promote the following replica?",
            "#{recommended.ip} (#{recommended.hostname})",
            "- yes/no -"
          ].join "\n"
          @promoted = recommended if agreed
        
          # choose a new node if they disagreed with our recommendation
          unless agreed
            choose do |promote|
              promote.prompt = 'Please choose a replica to promote:'
              @replicas.each do |replica|
                promote.choice "#{replica} - replication lag: #{replica.seconds_behind_master} seconds" do
                  @promoted = replica
                end
              end
            end
            raise "You chose a backup slave. These are not suitable for promotion. Please try again." if @promoted.for_backups?
          end
        end

        error "unable to establish node to promote" unless @promoted.kind_of? Jetpants::DB
      end
      
      def execute_promotion
        raise "Need to know which machine to demote and which to promote" unless @demoted && @promoted
        p = @demoted.pool
        
        # If there's no matching pool in the topology (such as if no asset tracker plugin
        # is in use), create a temporary one.  Give it a blank sync_configuration method
        # to ensure that the pool won't be written to any sort of config file.
        unless p
          p = Pool.new('temp-pool', @demoted)
          def p.sync_configuration; end
        end
        p.master_promotion! @promoted
      end

    end
  end
end