defmodule Leader do

  def start(config) do
    receive do
      { :BIND, acceptors, replicas } ->
        acceptors = MapSet.new(acceptors)
        ballot_num = {0, self()}
        active = false
        proposals = MapSet.new()
        spawn(Scout, :start, [config, self(), acceptors, ballot_num])
        next(config, acceptors, ballot_num, replicas, active, proposals)
    end
  end

  # y + (x - x-intersection-y)
  # i.e. take all elements from y, and the non-overlapping elements from x
  defp pref_union(x, y) do
    z = MapSet.difference(x, y)
    MapSet.union(z, y)
  end

  # returns (slot_num, command) such that the (b,s,c) they came from has b larger than any other b’ with same slot s and diff command c’.
  defp pmax(pvalues) do
    if MapSet.size(pvalues) > 0 do
      sorted = Enum.sort_by(pvalues, fn {b, _, _} -> b end)
      u = Enum.uniq_by(sorted, fn {_, s, _} -> s end)
      x = for {_, s, c} <- u do {s, c} end
      MapSet.new( x )
    else
      pvalues
    end
  end

  defp next(config, acceptors, ballot_num, replicas, active, proposals) do
    receive do
      {:PROPOSE, slot_num, command} ->
        proposals_at_slot = for {^slot_num, c_temp} <- proposals do {slot_num, c_temp} end
        if length(proposals_at_slot) == 0 do
          proposals = MapSet.put(proposals, {slot_num, command})
          if active do
            spawn(Commander, :start, [config, self(), acceptors, replicas, {ballot_num, slot_num, command}])
          end # if
          next(config, acceptors, ballot_num, replicas, active, proposals)
        else
          next(config, acceptors, ballot_num, replicas, active, proposals)
        end # if
      {:ADOPTED, ballot_num, pvalues} -> # TODO: see ballot_num --> must match?
        proposals = pref_union(proposals, pmax(pvalues))
        for {s_temp, c_temp} <- proposals do
          spawn(Commander, :start, [config, self(), acceptors, replicas, {ballot_num, s_temp, c_temp}]) # ballot num?
        end # for
        active = true
        next(config, acceptors, ballot_num, replicas, active, proposals)
      {:PREEMPTED, {r_new, leader_id_new}} ->
        if {r_new, leader_id_new} > ballot_num do



          active = false
          ballot_num = {r_new + 1, self()}
          spawn(Scout, :start, [config, self(), acceptors, ballot_num])
          next(config, acceptors, ballot_num, replicas, active, proposals)
        else
          next(config, acceptors, ballot_num, replicas, active, proposals)
        end # if
    end # receive
  end # next
end # module Leader
