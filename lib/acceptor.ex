
# Luca Mehl (lsm20)

defmodule Acceptor do

  def start(config) do
    ballot_num = -1  # -1 is intended as a falsity value for ballot_num
    accepted = MapSet.new()
    next(ballot_num, accepted)
  end

  defp next(ballot_num, accepted) do
    receive do
      {:p1a, leader_id, b} ->
        ballot_num = max(b, ballot_num)
        send leader_id, {:p1b, self(), ballot_num, accepted}
        next(ballot_num, accepted)

      {:p2a, leader_id, {b, s, c}} ->
        if b == ballot_num do
          accepted = MapSet.put(accepted, {b,s,c})
          send leader_id, {:p2b, self(), ballot_num}
          next(ballot_num, accepted)
        else
          send leader_id, {:p2b, self(), ballot_num}
          next(ballot_num, accepted)
        end # if
    end # receive
  end # next
end # module Acceptor
