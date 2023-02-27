defmodule Scout do

  def start(config, leader_id, acceptors, ballot_num) do
    send config.monitor, { :SCOUT_SPAWNED, config.node_num }
    pvalues = MapSet.new()
    waitfor = acceptors
    for a <- acceptors do
      send a, {:p1a, self(), ballot_num}
    end
    next(config, leader_id, acceptors, ballot_num, pvalues, waitfor)
  end

  defp next(config, leader_id, acceptors, ballot_num, pvalues, waitfor) do
    receive do
      {:p1b, acceptor_id, ballot_num_temp, r} -> # r is accepted set from acceptor
        if ballot_num_temp == ballot_num do
          pvalues = MapSet.union(pvalues, r)
          waitfor = MapSet.delete(waitfor, acceptor_id)
          if MapSet.size(waitfor) < MapSet.size(acceptors)/2 do
            send leader_id, {:ADOPTED, ballot_num, pvalues}
            send config.monitor, { :SCOUT_FINISHED, config.node_num }
            # exit
          else
            next(config, leader_id, acceptors, ballot_num, pvalues, waitfor)
          end # if
        else
          send leader_id, {:PREEMPTED, ballot_num_temp}
          send config.monitor, { :SCOUT_FINISHED, config.node_num }
          # exit
        end # if
    end # receive
  end # next
end # module Scout
