defmodule ApplicationProvider do
  @moduledoc """
  Documentation for ApplicationProvider.
  """

  alias ApplicationProvider.Access, as: AccessModule

  def init(ram_only \\true) do
    ApplicationProvider.Orchestration.init(ram_only)
  end

  @doc """
  Hello world.
  """
  def start(folder) do
    ApplicationProvider.Orchestration.start(folder)
  end

  @doc """
  Returns the comlete average of temperature and humidity for all time and stations.
  """
  def get_result(access_module) do
    AccessModule.get_result(access_module)
  end

  @doc """
  Returns the average temperature and humidity for the `year`.
  """
  def get_result(access_module, year) do
    AccessModule.get_result(access_module, year)
  end

  @doc """
  Returns the average temperature and humidity for the `month` in `year`.
  """
  def get_result(access_module, year, month) do
    AccessModule.get_result(access_module, year, month)
  end

  @doc """
  Returns the average temperature and humidity for the `day` of the `month` in `year`.
  """
  def get_result(access_module, year, month, day) do
    AccessModule.get_result(access_module, year, month, day)
  end

  @doc """
  Returns the average temperature and humidity for the `station`.
  """
  def get_station_result(access_module, station) do
    AccessModule.get_station_result(access_module, station)
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `year`.
  """
  def get_station_result(access_module, station, year) do
    AccessModule.get_station_result(access_module, station, year)
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `month` of the `year`.
  """
  def get_station_result(access_module, station, year, month) do
    AccessModule.get_station_result(access_module, station, year, month)
  end

  @doc """
  Returns the average temperature and humidity for the `station` on `day` of the `month` in `year`.
  """
  def get_station_result(access_module, station, year, month, day) do
    AccessModule.get_station_result(access_module, station, year, month, day)
  end

end
