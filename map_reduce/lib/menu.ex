  defmodule MapReduce.Menu do
  @moduledoc """
  Menu for the Batch Layer to comunicate with concurrent nodes
  """

  alias MapReduce.Data, as: Data

  @doc """
  Starting function for the MapReduce Server
  ## Examples

      iex> MapReduce.Menu.start()
  """
  def start do
    try do
      pid = spawn fn -> loop %Data{} end
      {:ok, pid}
    rescue
      ArgumentError -> :argument_error
      Error -> :error
    end
  end

  # Main loop of the Batch Layer
  defp loop(menu_data) do
    receive do
      {:receiver, :add, pid} ->
        add_receiver(menu_data, pid)
        |> loop
      {:receiver, :remove, pid} ->
        remove_receiver(menu_data, pid)
        |> loop
      # Add a node to Batch Layer
      {:node, :add, :map, key, pid} ->
        add_map_node(menu_data, key, pid)
        |> loop
      # Remove a Node
      {:node, :remove, :map, key, pid} ->
        remove_map_node(menu_data, key, pid)
        |> loop
      # Return List with all nodes
      {:node, :get, :map, pid} ->
        send(pid, {:ok, {:node, menu_data.map_nodes}})
        loop(menu_data)
      # Add a node to Batch Layer
      {:node, :add, :reduce, key, pid} ->
        add_reduce_node(menu_data, key, pid)
        |> loop
      # Remove a Node
      {:node, :remove, :reduce, key} ->
        remove_reduce_node(menu_data, key)
        |> loop
      # Return List with all nodes
      {:node, :get, :reduce, pid} ->
        send(pid, {:ok, {:node, menu_data.reduce_nodes}})
        loop(menu_data)
      # Add a result to the Batch Layer
      {:result, :add, :map, key, result} ->
        add_map_result(menu_data, key, result)
        |> check_map_process
        |> loop
      {:result, :add, :reduce, key, result} ->
        add_reduce_result(menu_data, key, result)
        |> check_reduce_process
        |> loop
      # Start calculation
      {:calculation, :start, data, pid} ->
        start_calculation(menu_data, data, pid)
        |> loop
      # Ping Node to check if it is running
      {:ping, pid} ->
        send(pid, :pong)
        loop(menu_data)
      # Print out the data of this module
      {:print, to_print} ->
        print(to_print, menu_data)
        loop(menu_data)
      # Shut down the Menu
      {:kill, pid} ->
        kill(menu_data, pid)
      any ->
        IO.puts("Unrecognized Message: MapReduce")
        IO.puts(inspect(any))
        loop(menu_data)
    end
  end

  #######################
  ### Functions for the main loop ###
  #######################
  # Adds a receiver node to the MapReduce Application
  defp add_receiver(menu_data, pid) do
    if pid in menu_data.receiver do
      menu_data
    else
      %Data{menu_data | receiver: [pid | menu_data.receiver]}
    end
  end

  # Removes a receiver node
  defp remove_receiver(menu_data, pid) do
    if pid in menu_data.receiver do
      %Data{menu_data | receiver: menu_data.receiver -- [pid]}
    else
      send(pid, {:map_reduce, :error, :receiver, :not_existing})
      menu_data
    end
  end

  #Adds a PID to the Map-Node-List
  defp add_map_node(menu_data, key, pid) do
    case Map.fetch(menu_data.map_nodes, key) do
      {:ok, nodes} ->
        if pid in nodes do
          menu_data
        else
          %Data{menu_data | map_nodes: %{menu_data.map_nodes | key => [pid | nodes]}}
        end
      :error ->
        %Data{menu_data | map_nodes: Map.put(menu_data.map_nodes, key, [pid])}
    end
  end

  # Removes a PID from the Map-Node-List
  defp remove_map_node(menu_data, key, pid) do
    case Map.fetch(menu_data.map_nodes, key) do
      {:ok, nodes} ->
        %Data{menu_data | map_nodes: %{menu_data.map_nodes | key => nodes -- [pid]}}
      :error ->
        menu_data
    end
  end

  #Adds a PID to the Map-Node-List
  defp add_reduce_node(menu_data, key, pid) do
    %Data{menu_data | reduce_nodes: Map.put(menu_data.reduce_nodes, key, pid)}
  end

  # Removes a PID from the Map-Node-List
  defp remove_reduce_node(menu_data, key) do
    %Data{menu_data | reduce_nodes: Map.drop(menu_data.reduce_nodes, [key])}
  end

  # Add a result to the result list
  defp add_map_result(menu_data, key, result) do
    case Map.fetch(menu_data.map_results, key) do
      {:ok, old} ->
        %Data{menu_data | map_results: Map.put(menu_data.map_results, key, [result | old])}
      :error ->
        %Data{menu_data | map_results: Map.put(menu_data.map_results, key, [result])}
    end
  end

  defp add_reduce_result(menu_data, key, result) do
    %Data{menu_data | reduce_results: Map.put(menu_data.reduce_results, key, result)}
  end

  # Starts the calculation
  defp start_calculation(menu_data, data, pid) do
    if menu_data.in_progress do
      send pid, {:map_reduce, :error, :in_progress}
    else
      used_map_nodes = Enum.into(
        Enum.map(
          Map.to_list(menu_data.map_nodes),
          fn {key, nodes} -> {key, start_calculation_nodes(data, nodes)} end
        ),
        Map.new()
      )
      %Data{menu_data | in_progress: true, used_map_nodes: used_map_nodes}
    end
  end

  # Print data
  defp print(to_print, data) do
    case to_print do
      :data ->
        IO.puts inspect(data)
      any ->
        case Map.fetch(data, any) do
          {:ok, obj} -> IO.puts inspect(obj)
          :error -> IO.puts "Unknown Key"
        end
    end
  end

  # Kills the Process
  defp kill(data, pid) do
    kill_nodes(data)
    send(pid, {:map_reduce, :ok, :kill})
  end

  # Kill all nodes
  defp kill_nodes(data) do
     Enum.each(data.nodes, fn node -> send node, :kill end)
  end

  ################
  ### Helper functions ###
  ################
  # Split Data for the Nodes
  defp split_data(_calculation_data, 0) do
    {:error, :no_node}
  end

  defp split_data(data, 1) do
    # if node_count is 1, just do calculation on whole list
    [data]
  end

  defp split_data(data, node_count) do
    chunk_size = trunc(Float.ceil(length(data) / node_count))
    # if chunk_size is bigger then min_data_count, send data to nodes
    if chunk_size > 10 do
      Enum.chunk_every(data, chunk_size)
    # if not, use one less node
    else
      split_data(data, node_count - 1)
    end
  end

  defp start_calculation_nodes(data, nodes) do
    case split_data(data, length(nodes)) do
      {:error, :no_node} ->
        []
      splitted ->
        send_data_to_nodes(splitted, nodes, [])
    end
  end

  # Send the splitted Data to the nodes
  defp send_data_to_nodes([], _nodes, used_nodes) do
    used_nodes
  end

  defp send_data_to_nodes([data_head | data_tail], [node_head | node_tail], used_nodes) do
    send(node_head, {:data, :calculate, :map, data_head})
    send_data_to_nodes(data_tail, node_tail, [node_head| used_nodes])
  end

  # Checks if every map node has sent a result
  defp check_map_process(menu_data) do
    keys = Map.keys(menu_data.used_map_nodes)
    finished_list = Enum.map(
      keys,
      fn key ->
        map_calculation_finished?(key, menu_data.map_results, menu_data.used_map_nodes)
      end
    )
    if false not in finished_list do
      send_map_results(menu_data)
    end
    menu_data
  end

  # Check if the reduction process of all map results is finished
  defp check_reduce_process(menu_data) do
    if length(Map.keys(menu_data.reduce_results)) == length(Map.keys(menu_data.reduce_nodes)) do
      send_reduce_results(menu_data)
      %Data{menu_data | in_progress: false, map_results: %{}, reduce_results: %{}}
    else
      menu_data
    end
  end

  # Check if calculation is finished
  defp map_calculation_finished?(key, results, used_nodes) do
    case Map.fetch(results, key) do
      {:ok, res} ->
        case Map.fetch(used_nodes, key) do
          {:ok, nodes} ->
            length(res) == length(nodes)
          :error ->
            false
        end
      :error ->
        false
    end
  end

  # Send map-results to reduce nodes
  defp send_map_results(menu_data) do
    Enum.each(
      Map.keys(menu_data.map_results),
      fn key ->
        send_map_to_reduce(key, menu_data.map_results, menu_data.reduce_nodes)
      end
    )
  end

  # Sends Results from Map Nodes to the Reduce Nodes
  defp send_map_to_reduce(key, map_results, reduce_nodes) do
    case Map.fetch(map_results, key) do
      {:ok, results} ->
        case Map.fetch(reduce_nodes, key) do
          {:ok, node} ->
            send(node, {:data, :calculate, :reduce, results})
          :error ->
            false
        end
      :error ->
        false
    end
  end

  defp send_reduce_results(menu_data) do
    Enum.each(
      menu_data.receiver,
      fn receiver ->
        send(receiver, {:map_reduce, :result, menu_data.reduce_results})
      end
    )
  end

end # Module end
