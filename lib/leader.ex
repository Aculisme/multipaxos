
# Luca Mehl (lsm20)

defmodule Leader do

  def start(config) do
    receive do
      { :BIND, acceptors, replicas } ->
        acceptors = MapSet.new(acceptors)
        ballot_num = {0, self()}
        active = false
        proposals = MapSet.new()
        timeout = 100 # ms
        spawn(Scout, :start, [config, self(), acceptors, ballot_num])
        next(config, acceptors, ballot_num, replicas, active, proposals, timeout)
    end
  end

  defp update(x, y) do
    diff = MapSet.new(for {s, c} <- x, !Enum.find(y, fn p -> match?({^s, _}, p) end), do: {s, c})
    MapSet.union(diff, y)
  end

  # returns (slot_num, command) such that the (b,s,c) they came from has b larger than any other b’ with same slot s and diff command c’.
  defp pmax(pvalues) do
  #   MapSet.new(for {b, s, c} <- pvalues, Enum.all?(pvalues, fn {b_temp, s_temp, _} -> s != s_temp or b_temp <= b end), do: {b, s, c})
    if MapSet.size(pvalues) > 0 do
      sorted = Enum.sort_by(pvalues, fn {b, _, _} -> b end)
      u = Enum.uniq_by(sorted, fn {_, s, _} -> s end)
      x = for {_, s, c} <- u do {s, c} end
      MapSet.new( x )
    else
      pvalues
    end
  end

  defp next(config, acceptors, ballot_num, replicas, active, proposals, timeout) do
    receive do
      {:PROPOSE, slot_num, command} ->
        proposals_at_slot = for {^slot_num, c_temp} <- proposals do {slot_num, c_temp} end
        if length(proposals_at_slot) == 0 do
          proposals = MapSet.put(proposals, {slot_num, command})
          if active do
            spawn(Commander, :start, [config, self(), acceptors, replicas, {ballot_num, slot_num, command}])
          end # if
          next(config, acceptors, ballot_num, replicas, active, proposals, timeout)
        else
          next(config, acceptors, ballot_num, replicas, active, proposals, timeout)
        end # if

      {:ADOPTED, ^ballot_num, pvalues} ->

        # Success, linearly decrease timeout for next attempt
        timeout = max(timeout - 50, 0)

        proposals = update(proposals, pmax(pvalues))
        for {s_temp, c_temp} <- proposals do
          spawn(Commander, :start, [config, self(), acceptors, replicas, {ballot_num, s_temp, c_temp}])
        end # for
        active = true

        next(config, acceptors, ballot_num, replicas, active, proposals, timeout)

      {:PREEMPTED, {r_new, leader_id_new}} ->
        if {r_new, leader_id_new} > ballot_num do

          # Discovers there is another ballot in circulation with higher number,
          # give the other leader time to complete
          Process.sleep(timeout)

          # Multiplicatively increase timeout for next attempt
          timeout = min(round(timeout * 1.2), 10_000)

          active = false
          ballot_num = {r_new + 1, self()}
          spawn(Scout, :start, [config, self(), acceptors, ballot_num])
          next(config, acceptors, ballot_num, replicas, active, proposals, timeout)
        else
          next(config, acceptors, ballot_num, replicas, active, proposals, timeout)
        end # if
    end # receive
  end # next
end # module Leader
