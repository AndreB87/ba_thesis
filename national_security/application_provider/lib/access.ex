defmodule ApplicationProvider.Access do

  @moduledoc """
  Serving Layer for the Lambda Architecture.
  Serves the Results from Batch and Speed Layer.
  """

  @doc """
  Returns the comlete average of temperature and humidity for all time and stations.
  """
  def get_result(mnesia_pid) do
    send(mnesia_pid, {:get, :result, :avg_complete, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `year`.
  """
  def get_result(mnesia_pid, year) do
    send(mnesia_pid, {:get, :result, {:year, year}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `month` in `year`.
  """
  def get_result(mnesia_pid, year, month) do
    send(mnesia_pid, {:get, :result, {:month, year, month}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `day` of the `month` in `year`.
  """
  def get_result(mnesia_pid, year, month, day) do
    send(mnesia_pid, {:get, :result, {:day, year, month, day}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `station`.
  """
  def get_station_result(mnesia_pid, station) do
    send(mnesia_pid, {:get, :result, {:station, station}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `year`.
  """
  def get_station_result(mnesia_pid, station, year) do
    send(mnesia_pid, {:get, :result, {:station, station, year}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `month` of the `year`.
  """
  def get_station_result(mnesia_pid, station, year, month) do
    send(mnesia_pid, {:get, :result, {:station, station, year, month}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `station` on `day` of the `month` in `year`.
  """
  def get_station_result(mnesia_pid, station, year, month, day) do
    send(mnesia_pid, {:get, :result, {:station, station, year, month, day}, self()})
    receive_fun()
  end

  defp receive_fun do
    receive do
      any ->
        any
    end
  end

end
