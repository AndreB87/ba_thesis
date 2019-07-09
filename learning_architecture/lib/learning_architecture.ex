defmodule LearningArchitecture do
  @moduledoc """
  Documentation for LearningArchitecture.
  """

  alias LearningArchitecture.Data, as: Data

  @doc """
  Initialises the Mnesia Database for storing the results.
  If `ram_only` is true, the results are stored not persistent
  """
  def init(ram_only \\true) do
     DataManagement.init(ram_only)
  end

  @doc """
  Starts the Learning Architecture
  """
  def start do
    {:ok, data_management} = DataManagement.start()
    %Data{data_management: data_management}
  end

  @doc """
  Starts the Data Gathering Service
  """
  def read_data(la_data, folder) do
    DataGathering.start(la_data.data_manager, folder)
  end

  @doc """
  Returns the comlete average of temperature and humidity for all time and stations.
  """
  def get_result(la_data) do
    DataManagement.get_result(la_data.data_management, :avg_complete)
  end

  @doc """
  Returns the average temperature and humidity for the `year`.
  """
  def get_result(la_data, year) do
    DataManagement.get_result(la_data.data_management, {:year, year})
  end

  @doc """
  Returns the average temperature and humidity for the `month` in `year`.
  """
  def get_result(la_data, year, month) do
    DataManagement.get_result(la_data.data_management, {:month, year, month})
  end

  @doc """
  Returns the average temperature and humidity for the `day` of the `month` in `year`.
  """
  def get_result(la_data, year, month, day) do
    DataManagement.get_result(la_data.data_management, {:day, year, month, day})
  end

  @doc """
  Returns the average temperature and humidity for the `station`.
  """
  def get_station_result(la_data, station) do
    DataManagement.get_result(la_data.data_management, {:station, station})
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `year`.
  """
  def get_station_result(la_data, station, year) do
    DataManagement.get_result(la_data.data_management, {:station, station, year})
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `month` of the `year`.
  """
  def get_station_result(la_data, station, year, month) do
    DataManagement.get_result(la_data.data_management, {:station, station, year, month})
  end

  @doc """
  Returns the average temperature and humidity for the `station` on `day` of the `month` in `year`.
  """
  def get_station_result(la_data, station, year, month, day) do
    DataManagement.get_result(la_data.data_management, {:station, station, year, month, day})
  end

end
