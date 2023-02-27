
# Luca Mehl (lsm20)

defmodule Commander do

  def start(config, leader_id, acceptors, replicas, {ballot_num, slot_num, command}) do
    send config.monitor, { :COMMANDER_SPAWNED, config.node_num }
    waitfor = acceptors
    for a <- acceptors do
      send a, {:p2a, self(), {ballot_num, slot_num, command}}
    end # for
    next(config, leader_id, acceptors, ballot_num, waitfor, replicas, slot_num, command) # todo: change ordering?
  end # start

  defp next(config, leader_id, acceptors, ballot_num, waitfor, replicas, slot_num, command) do
    receive do
      {:p2b, acceptor_id, ballot_num_temp} ->
        if ballot_num_temp == ballot_num do
          waitfor = MapSet.delete(waitfor, acceptor_id)
          if MapSet.size(waitfor) < MapSet.size(acceptors)/2 do
            for replica <- replicas do
              send replica, {:DECISION, slot_num, command}
            end # for
            send config.monitor, { :COMMANDER_FINISHED, config.node_num }
            # exit
          else
            next(config, leader_id, acceptors, ballot_num, waitfor, replicas, slot_num, command)
          end # if
        else
          send leader_id, {:PREEMPTED, ballot_num_temp}
          send config.monitor, { :COMMANDER_FINISHED, config.node_num }
          # exit
        end # if
    end # receive
  end # next
end # module Commander
