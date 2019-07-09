defmodule BatchProcessing do
  @moduledoc """
  Batch Processing Module to Calculate Results of Temperature Data.
  """

  alias BatchProcessing.Cassandra, as: Cassandra

  @doc """
  Starts the Batch Processing Service. If Service was started successfully,
  it will return `{:ok, pid}`` where `pid` is the PID of the Batch Processing Service
  """
  def start() do
    db_host = Application.get_env(:batch_processing, :db_host)
    {:ok, db_pid} = init_db(db_host)
    {:ok, mr_pid} = init_map_reduce()
    BatchProcessing.Server.start(db_pid, mr_pid)
  end

  @doc """
  Stops the Batch Processing Service with the given `server_pid`
  """
  def stop(server_pid) do
    send(server_pid, :kill)
  end

  @doc """
  Adds temperature `data` to the Batch Processing Service with the `server_pid`
  """
  def add_data(server_pid, data) do
    send(server_pid, {:data, :add, data})
  end

  @doc """
  Adds the `data_list` of temperature data to the Batch Processing Service
  with the `server_pid`
  """
  def add_many(server_pid, data_list) do
    send(server_pid, {:data, :add_many, data_list})
  end

  @doc """
  Corrects the temperature `data` in the Batch Processing Service with the
  `server_pid`
  """
  def correct_data(server_pid, data) do
    send(server_pid, {:data, :correct, data})
  end

  @doc """
  Deletes the temperature data regarding to `year`, `month`, `day`, `hour`
  and `station` from the Batch Processing Service with the `server_pid`
  """
  def delete_data(server_pid, station, year, month, day, hour) do
    send(server_pid, {:data, :delete, station, year, month, day, hour})
  end

  @doc """
  Returns the data set regarding to `station`, `year`, `month`, `day` and `hour`
  from the Batch Processing Service with the `server_pid`
  """
  def get_data(server_pid, station, year, month, day, hour) do
    send(server_pid, {:data, :get, station, year, month, day, hour, self()})
    receive do
      {:batch_processing, :data, temp_data} ->
        temp_data

      _any ->
        :error
    end
  end

  @doc """
  Adds a map or reduce node to the Batch Processing Service
  `type` -> :map or :reduce
  `key` -> key of the function
  `function` -. reference to the map or reduce function
  """
  def add_node(server_pid, type, key, function) do
    send server_pid, {:node, :add, type, key, function}
  end

  @doc """
  Adds the `receiver_pid` to receivce results from the Batch Processing Service
  with the `server_pid`
  """
  def add_receiver(server_pid, receiver_pid) do
    send(server_pid, {:receiver, :add, receiver_pid})
  end

  # Initializes the Database
  defp init_db(db_host) do
    case Cassandra.open(db_host) do
      {:ok, conn} ->
        {:ok, conn}
      {:error, error} ->
        {:error, error}
    end
  end

  # Initializes the MAp Reduce Service
  defp init_map_reduce() do
    case MapReduce.start() do
      {:ok, pid} ->
        {:ok, pid}
      _any ->
        {:error, :map_reduce}
    end
  end

end
