#!/opt/ruby/2.3/bin/ruby

#
# Receiver:
#
# This ruby script is fork-exec'ed by 'ncat' (similar to xinetd) utility when it receives a
# connection request on the port it is listening on.  The network socket on which connection
# is accepted becomes the STDIN for the script.  This script receives the data sent by the
# sender as a part of cloning process and relays it to any further targets (chain_ip:chain_port).
# Data transfer is controlled by the parameters in 'param_file'.  Based on the
# parameters, a simple command chain "decryption | decompression | dd" is used for the transfer.
#
# Jetpants have started the parent 'ncat' process with '--sh-exec' this script.  So, jetpants
# does not really start this script, but a connection request from sender to ncat port would
# exec this script.
#
# When receiver has successfully read the parameters and built the command line, it cerates a
# marker at /db-binlog/<some_id>.success.  That marker is indication to Jetpants that the transfer
# has almost started.  Based on these markers, Jetpants orchestrate the further transfers.
#
require 'yaml'
require 'time'
require 'logger'

param_file = "/db-binlog/__ncat_clone_recv_params.yaml"
transfer_parameters = YAML.load(File.read(param_file))
log_file = "/var/log/jetpants_clone_receiver.log"

io_size = 524288
# Sanity checks
raise "'base_dir' parameter missing in parameter file" unless transfer_parameters.key?('base_dir')
raise "'filename' parameter missing in parameter file" unless transfer_parameters.key?('filename')
raise "'block_count' parameter missing in parameter file"  unless transfer_parameters.key?('block_count')
raise "'block_offset' parameter missing in parameter file" unless transfer_parameters.key?('block_offset')
raise "'transfer_id' parameter missing in parameter file"  unless transfer_parameters.key?('transfer_id')
base_dir     = transfer_parameters['base_dir']
filename     = transfer_parameters['filename']
block_count  = transfer_parameters['block_count'].to_i
block_offset = transfer_parameters['block_offset'].to_i
transfer_id  = transfer_parameters['transfer_id']

read_size  = transfer_parameters['read_size']  || io_size
block_size = transfer_parameters['block_size'] || io_size

logger = Logger.new(log_file)
logger.formatter = proc do |severity, datetime, progname, msg|
  "[#{datetime}][#{transfer_id}] #{severity} #{msg}\n"
end

# Build the 'dd' command
recv_dd = "dd of=#{filename} bs=#{block_size} seek=#{block_offset} 2>/dev/null"
logger.info("recv dd = #{recv_dd}")

# How to decompress and decrypt?
decompression_stage = ''
decompression_stage = "#{transfer_parameters['decompression_cmd']} | " if transfer_parameters['decompression_cmd']

decryption_stage = ''
decryption_stage = "#{transfer_parameters['decryption_cmd']} | " if transfer_parameters['decryption_cmd']

# Chaining: Do we want to forward the data to some other target?
chaining_enabled = false
if transfer_parameters.key?('chain_ip')
  raise "'chain_port' parameter missing in parameter file" unless transfer_parameters.key?('chain_port')
  chain_ip   = transfer_parameters['chain_ip']
  chain_port = transfer_parameters['chain_port'].to_i

  chain_cmd = "ncat --send-only #{chain_ip} #{chain_port}"
  chained_target = IO.popen(chain_cmd, 'wb')
  chaining_enabled = true
  logger.info("Chaining to #{chain_ip}:#{chain_port} enabled")
end

# Tell Jetpants we have successfully read the parameters and it can overwrite those values
success_cmd = "cat #{param_file} > /db-binlog/__#{transfer_id}.success"
%x[ #{success_cmd} ]
logger.info("Marker created")

# The pipeline of receiver
final_cmd = "cd #{base_dir} && #{decryption_stage} #{decompression_stage} #{recv_dd}"

sum_size = 0
sum_ops = 0

# Fire it
target = IO.popen(final_cmd, 'wb')
loop do
  blob = STDIN.read(read_size)
  target << blob
  chained_target << blob if chaining_enabled
  sum_size += blob.length
  sum_ops += 1
  break if blob.length < read_size
end

target.close
chained_target.close if chaining_enabled
logger.info("Done, Size: #{sum_size} Ops: #{sum_ops}")
