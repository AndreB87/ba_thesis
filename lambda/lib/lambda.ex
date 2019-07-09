defmodule Lambda do
  @moduledoc """
  Documentation for Lambda.
  """

  alias Lambda.Data, as: Data

  @doc """
  Hello world.

  ## Examples

      iex> Lambda.hello()
      :world

  """
  def init(ram_only \\true) do
    ServingLayer.init(ram_only)
  end

  def start do
     {:ok, serving_layer} = ServingLayer.start()
     {:ok, batch_layer} = BatchLayer.start(serving_layer)
     {:ok, speed_layer} = SpeedLayer.start(serving_layer)
     {:ok, %Data{serving_layer: serving_layer, batch_layer: batch_layer, speed_layer: speed_layer}}
  end

  def read_data(lambda_data, folder) do
    BatchLayer.DataReader.start(lambda_data.batch_layer, folder)
    SpeedLayer.DataReader.start(lambda_data.speed_layer, folder)
  end

  def stop(lambda_data) do
    BatchLayer.stop(lambda_data.batch_layer)
    SpeedLayer.stop(lambda_data.speed_layer)
    ServingLayer.stop(lambda_data.serving_layer)
  end

  @doc """
  Returns the comlete average of temperature and humidity for all time and stations.
  """
  def get_result(lambda_data) do
    ServingLayer.get_result(lambda_data.serving_layer)
  end

  @doc """
  Returns the average temperature and humidity for the `year`.
  """
  def get_result(lambda_data, year) do
    ServingLayer.get_result(lambda_data.serving_layer, year)
  end

  @doc """
  Returns the average temperature and humidity for the `month` in `year`.
  """
  def get_result(lambda_data, year, month) do
    ServingLayer.get_result(lambda_data.serving_layer, year, month)
  end

  @doc """
  Returns the average temperature and humidity for the `day` of the `month` in `year`.
  """
  def get_result(lambda_data, year, month, day) do
    ServingLayer.get_result(lambda_data.serving_layer, year, month, day)
  end

  @doc """
  Returns the average temperature and humidity for the `station`.
  """
  def get_station_result(lambda_data, station) do
    ServingLayer.get_station_result(lambda_data.serving_layer, station)
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `year`.
  """
  def get_station_result(lambda_data, station, year) do
    ServingLayer.get_station_result(lambda_data.serving_layer, station, year)
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `month` of the `year`.
  """
  def get_station_result(lambda_data, station, year, month) do
    ServingLayer.get_station_result(lambda_data.serving_layer, station, year, month)
  end

  @doc """
  Returns the average temperature and humidity for the `station` on `day` of the `month` in `year`.
  """
  def get_station_result(lambda_data, station, year, month, day) do
    ServingLayer.get_station_result(lambda_data.serving_layer, station, year, month, day)
  end

end
