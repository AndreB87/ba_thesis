defmodule StreamProcessing do
  @moduledoc """
  Stream Processing Module to Calculate Results of Temperature Data.
  """

  alias StreamProcessing.Server, as: Server

  @doc """
  Starts the Stream Processing Module
  """
  def start() do
    Server.start()
  end

  @doc """
  Stops the Stream Processing Service with the `sp_pid`
  """
  def stop(sp_pid) do
    send(sp_pid, :kill)
  end

  @doc """
  Adds `temp_data` to the Stream Processing Service with the `sp_pid`
  """
  def add_data(sp_pid, temp_data) do
    send(sp_pid, {:data, :add, temp_data})
  end

  @doc """
  Adds a list of temperature data to the Stream Processing Service with the `sp_pid`
  """
  def add_many(sp_pid, [head | tail]) do
    send(sp_pid, {:data, :add_many,  [head | tail]})
  end

  @doc """
  Adds a `receiver` to receive Results from the Stream Processing Service with
  the `sp_pid`
  """
  def add_receiver(sp_pid, receiver) do
    send(sp_pid, {:receiver, :add, receiver})
  end

  @doc """
  Removes a `receiver` from the Stream Processing Service with the `sp_pid`
  """
  def remove_receiver(sp_pid, receiver) do
    send(sp_pid, {:receiver, :remove, receiver})
  end

  @doc """
  Removes a Result with the `result_key` from the Stream Processing Service
  with the `sp_pid`
  """
  def remove_from_result(sp_pid, result_key) do
    send(sp_pid, {:result, :remove, result_key})
  end

  @doc """
  Resets all results from the Stream Processing Service with the `sp_pid`
  """
  def reset_result(sp_pid) do
    send(sp_pid, {:result, :reset})
  end

end
