defmodule Replica do

  def start(config, database) do
    slot_in = 1
    slot_out = 1
    requests = []
    proposals = []
    decisions = []
    receive do
      { :BIND, leaders } ->
        next(config, decisions, slot_in, slot_out, proposals, requests, leaders, database)
    end
  end

  defp propose(config, requests, slot_in, slot_out, proposals, decisions, leaders) do
    window = config.window_size
    if slot_in < (slot_out + window) and length(requests) > 0 do
      c = hd(requests)
      decisions_at_slot_in = for {^slot_in, c_temp} <- decisions do {slot_in, c_temp} end
      if length(decisions_at_slot_in) == 0 do
        requests = List.delete(requests, c)
        proposals = proposals ++ [c]
        for leader_id <- leaders do
          send leader_id, {:PROPOSE, slot_in, c}
        end
        slot_in = slot_in + 1
        propose(config, requests, slot_in, slot_out, proposals, decisions, leaders) # return?
      else
        slot_in = slot_in + 1
        propose(config, requests, slot_in, slot_out, proposals, decisions, leaders) # return?
      end # if
    else # if (while loop finished)
      {requests, slot_in, proposals}
    end # if
  end # defp propose


  defp perform(slot_out, decisions, {k, cid, op}, database) do
    command_matches = Enum.filter(decisions, fn {slot_temp, {k_temp, cid_temp, op_temp}} -> slot_temp < slot_out and k_temp == k and cid_temp == cid and op_temp == op end)
    if length(command_matches) > 0 do
      slot_out = slot_out + 1
      {slot_out, decisions, {k, cid, op}, database}
    else
      send database, { :EXECUTE, op }
      slot_out = slot_out + 1
      {slot_out, decisions, {k, cid, op}, database}
    end
  end


  defp decision_helper(decisions, slot_out, slot_num, command, proposals, requests, database) do
    decisions_at_slot_out = for {^slot_out, c_temp} <- decisions do c_temp end
    if length(decisions_at_slot_out) > 0 do
      c_temp = hd(decisions_at_slot_out)
      proposals_at_slot_out = for {^slot_out, c_temp_2} <- proposals do c_temp_2 end
      if length(proposals_at_slot_out) > 0 do
        c_temp_2 = hd(proposals_at_slot_out)
        proposals = List.delete(proposals, {slot_out, c_temp_2})
        if c_temp_2 !== c_temp do
          requests = requests ++ [c_temp_2]
          {slot_out, decisions, command, database} = perform(slot_out, decisions, command, database)
          decision_helper(decisions, slot_out, slot_num, command, proposals, requests, database)
        else
          {slot_out, decisions, command, database} = perform(slot_out, decisions, command, database)
          decision_helper(decisions, slot_out, slot_num, command, proposals, requests, database)
        end # if
      else
        {slot_out, decisions, command, database} = perform(slot_out, decisions, command, database)
        decision_helper(decisions, slot_out, slot_num, command, proposals, requests, database)
      end # if
    else
      {decisions, slot_out, proposals, requests} # return once length(decisions at slot out) == 0
    end # if
  end # decision_helper

  defp next(config, decisions, slot_in, slot_out, proposals, requests, leaders, database) do
    receive do
      {:CLIENT_REQUEST, command} ->
        send config.monitor, { :CLIENT_REQUEST, config.node_num }
        requests = requests++[command]
        {requests, slot_in, proposals} = propose(config, requests, slot_in, slot_out, proposals, decisions, leaders)
        next(config, decisions, slot_in, slot_out, proposals, requests, leaders, database)
      {:DECISION, slot_num, command} ->
        decisions = decisions ++ [{slot_num, command}]
        {decisions, slot_out, proposals, requests} = decision_helper(decisions, slot_out, slot_num, command, proposals, requests, database)
        {requests, slot_in, proposals} = propose(config, requests, slot_in, slot_out, proposals, decisions, leaders)
        next(config, decisions, slot_in, slot_out, proposals, requests, leaders, database)
    end # receive
  end # next
end # module Replica
