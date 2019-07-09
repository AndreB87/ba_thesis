defmodule MnesiaServer do
  @moduledoc """
  Starts a new Mnesia Server.
  """

  def start do
    MnesiaServer.Server.start()
  end

  def stop(server_pid) do
    send(server_pid, :kill)
  end

  @doc """
  Initializes the database, must be executed only once per node
  """
  def init(server_pid) do
    send(server_pid, :init)
  end

  def create_table(server_pid, table, columns, ram_only) do
    send(server_pid, {:create_table, table, columns, ram_only, self()})
    receive do
      {:mnesia, result} ->
        result
    end
  end

  def create_index(server_pid, table, index) do
    send(server_pid, {:create_index, table, index, self()})
    receive do
      {:mnesia, result} ->
        result
    end
  end

  def add_column(server_pid, table, column) do
    send(server_pid, {:add_column, table, column, self()})
    receive do
      {:mnesia, result} ->
        result
    end
  end

  def read(server_pid, table, id) do
    send(server_pid, {:read, table, id, self()})
    receive do
      {:mnesia, result} ->
        result
    end
  end

  def index_read(server_pid, table, column, value) do
    send(server_pid, {:index_read, table, column, value, self()})
    receive do
      {:mnesia, result} ->
        result
    end
  end

  def match_object(server_pid, table, match_list) do
    send(server_pid, {:match_object, table, match_list, self()})
    receive do
      {:mnesia, result} ->
        result
    end
  end

  def write(server_pid, table, data) do
    send(server_pid, {:write, table, data, self()})
    receive do
      {:mnesia, result} ->
        result
    end
  end

end
