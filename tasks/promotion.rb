module Jetpants
  module Tasks
    class Promotion

      def initialize nodes = {}
        @demoted  = nodes['demote']
        @promoted = nodes['promote']
        super
        Jetpants.verify_replication = false # since master may be offline
        advise
        establish_roles
        prepare
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
 
      def advise
        @states = {
          preparing:  "processing promotion requirements",
          prepared:   "preparing to disable writes on #{@demoted}",
          read_only:  "writes have been disabled on #{@demoted}, preparing to demote #{@demoted} and promote #{@promoted}",
          promoted:   "#{@promoted} has been promoted, please prepare database config for deploy.",
          deployable: "promotion is complete, please commit and deploy.",
        }
        inform @states[@state.to_sym]
      end
  
      state_machine :initial => :preparing do
        after_transition any => any, :do => :advise
      
        event :prepare do
          transition :preparing => :prepared, :if => :roles_populated?
        end
        after_transition :preparing => :prepared, :do => :disable_writes

        event :disable_writes do
          transition :prepared  => :read_only, :if => :read_only!
        end
        after_transition :prepared => :read_only, :do => :promote
      
        event :promote do
          transition :read_only => :promoted, :if => :execute_promotion
        end
        after_transition :read_only => :promoted, :do => :prepare_config
      
        event :prepare_config do
          transition :promoted => :deployable, :if => :nodes_consistent? 
        end
        after_transition :promoted => :deployable, :do => :summarize_promotion
        
        state :preparing, :prepared do
          def is_db? node
            node.kind_of? Jetpants::DB
          end
          
          def roles_populated?
            # ensure our roles are populated with dbs
            [@demoted, @promoted, @replicas].all? do |role|
              is_db? role or role.all? do |node|
                is_db? node
              end
            end
          end
    
          def read_only!
            unless @demoted.available?
              status = @promoted.slave_status
              @log, @position = status[:master_log_file], status[:exec_master_log_pos].to_i
              return true
            end

            # set read_only if needed
            @demoted.read_only! unless @demoted.read_only?
            # bail if we're unable to set read_only
            error "unable to set 'read_only' on #{@demoted}" unless @demoted.read_only?
            # record the current log possition to ensure writes are not taking place later.
            @log, @position = @demoted.binlog_coordinates
            error "#{@demoted} is still taking writes, unable to promote #{@promoted}" unless writes_disabled?
            @demoted.read_only?            
          end

          def writes_disabled?
            return true unless @demoted.available?

            # ensure no writes have been logged since read_only!
            [@log, @position] == @demoted.binlog_coordinates
          end
          
        end

        state :read_only, :promoted, :promoted, :deployable do
          def nodes_consistent?
            return true unless @demoted.available?
            @replicas.all? {|replica| replica.slave_status[:exec_master_log_pos].to_i == @position}
          end

          def ensure_nodes_consistent?
            inform "ensuring replicas are in a consistent state"
            until nodes_consistent? do
              print '.'
              sleep 0.5
            end
            nodes_consistent?
          end

          def promotable?
            disable_replication if ensure_nodes_consistent? and @promoted.disable_read_only! 
          end

          def execute_promotion
            error 'nodes are not in a promotable state.' unless promotable?
            error 'replicas are not in a consistent state' unless nodes_consistent? 
            p = @demoted.pool || Pool.new('temp-pool', @demoted)
            p.master_promotion! @promoted
          end
          
          def replicas_replicating? replicas = @replicas
            replicas.all? {|replica| replica.replicating?}
          end

          def disable_replication replicas = @replicas
            replicas.each do |replica|
              replica.pause_replication if replica.replicating?
            end
            not replicas_replicating? replicas
          end

          def summarize_promotion transition
            summary = Terminal::Table.new :title => 'Promotion Summary:' do |rows|
              rows << ['demoted',  @demoted]
              rows << ['promoted', @promoted]
              rows << ["replicas of #{@promoted}", @promoted.slaves.join(', ')]
            end
            puts summary
            exit
          end
        end
      end

    end
  end
end