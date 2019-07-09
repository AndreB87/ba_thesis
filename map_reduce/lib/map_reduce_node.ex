defmodule MapReduce.MapReduceNode do

  @doc """
  Starts the Map Reduce Node with the given `type`, `key` and 'function'
  """
  def start(map_reduce, type, key, function) do
    if (type == :map) or (type == :reduce) do
      pid = spawn(fn -> loop(map_reduce, type, key, function) end)
      send(map_reduce, {:node, :add, type, key, pid})
      {:ok, pid}
    else
      :error
    end
  end

  # Main Loop of the Map/Reduce Node
  defp loop(menu_node, type, key, function) do
    receive do
      {:data, :calculate, type, data} ->
        send(menu_node, {:result, :add, type, key, function.(data)})
        loop(menu_node, type, key, function)
      {:ping, pid} ->
        send(pid, :pong)
        loop(menu_node, type, key, function)
      {:kill, pid} ->
        send(menu_node, {:node, :remove, self()})
        send(pid, {:kill, :ok})
      :kill ->
        send(menu_node, {:node, :remove, self()})
        :ok
      any ->
        IO.puts("\nUnknown Message: MR-Node")
        IO.puts inspect any
        loop(menu_node, type, key, function)
    end
  end

end
