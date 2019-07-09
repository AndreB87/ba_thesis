defmodule MapReduce do

  @moduledoc """
  Map Reduce Algorithm implemented for the Bachelor Thesis of Andre Brand
  """

  alias MapReduce.MapReduceNode, as: MapReduceNode
  alias MapReduce.Menu, as: Menu

  @doc """
  Starts the Application
  """
  def start do
    case Menu.start() do
      {:ok, pid} ->
        {:ok, pid}
      :argument_error ->
        IO.puts "An Error occured while starting the MapReduce Program - maybe another Proces is running on this node"
        :argument_error
      :error ->
        IO.puts "Error while starting the menu"
        :error
    end
  end

  @doc """
  Stops the MapReduce Server
  """
  def stop(map_reduce) do
    send(map_reduce, {:kill, self()})
    receive do
      {:ok, :kill} ->
        IO.puts "Application successfully terminated"
        :ok
      _else ->
        IO.puts "Application was not terminated successfully"
        :error
      after 500 ->
        :timeout
    end
  end

  @doc """
  Register a node as a receiver
  """
  def register(map_reduce, pid) do
    send(map_reduce, {:receiver, :add, pid})
    :ok
  end

  def add_node(map_reduce, type, key, function) do
     MapReduceNode.start(map_reduce, type, key, function)
  end

  @doc """
  Send a ping to the menu
  """
  def ping_menu(map_reduce) do
    send(map_reduce, {:ping, self()})
    receive do
      :pong -> :pong
      _else -> :pang
    after 500 -> :pang
    end
  end

end
