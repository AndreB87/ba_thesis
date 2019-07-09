defmodule NationalSecurity do
  @moduledoc """
  Documentation for NationalSecurity.
  """

  def init(ram_only \\true) do
    ApplicationProvider.init(ram_only)
  end

  @doc """
  Hello world.
  """
  def start do
    ApplicationProvider.start("../../Daten/")
  end

  @doc """
  Returns the comlete average of temperature and humidity for all time and stations.
  """
  def get_result(access_module) do
    ApplicationProvider.get_result(access_module)
  end

  @doc """
  Returns the average temperature and humidity for the `year`.
  """
  def get_result(access_module, year) do
    ApplicationProvider.get_result(access_module, year)
  end

  @doc """
  Returns the average temperature and humidity for the `month` in `year`.
  """
  def get_result(access_module, year, month) do
    ApplicationProvider.get_result(access_module, year, month)
  end

  @doc """
  Returns the average temperature and humidity for the `day` of the `month` in `year`.
  """
  def get_result(access_module, year, month, day) do
    ApplicationProvider.get_result(access_module, year, month, day)
  end

  @doc """
  Returns the average temperature and humidity for the `station`.
  """
  def get_station_result(access_module, station) do
    ApplicationProvider.get_station_result(access_module, station)
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `year`.
  """
  def get_station_result(access_module, station, year) do
    ApplicationProvider.get_station_result(access_module, station, year)
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `month` of the `year`.
  """
  def get_station_result(access_module, station, year, month) do
    ApplicationProvider.get_station_result(access_module, station, year, month)
  end

  @doc """
  Returns the average temperature and humidity for the `station` on `day` of the `month` in `year`.
  """
  def get_station_result(access_module, station, year, month, day) do
    ApplicationProvider.get_station_result(access_module, station, year, month, day)
  end

end
