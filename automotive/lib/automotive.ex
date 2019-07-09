defmodule Automotive do
  @moduledoc """
  Documentation for Automotive.
  """

  alias Automotive.Data, as: Data

  @doc """
  Initializes the Automotive Application.

  ## Examples

      iex> Automotive.init()
      :world

  """
  def init(ram_only \\true) do
    ServingLayer.init(ram_only)
  end

  def start() do
    {:ok, mq} = MessageQueue.start()
    {:ok, serving_layer} = ServingLayer.start(mq)
    BatchLayer.start(
      mq,
      {
        Application.get_env(:batch_processing, :name),
        Application.get_env(:batch_processing, :node)
      }
    )
    SpeedLayer.start(
      mq,
      {
        Application.get_env(:stream_processing, :name),
        Application.get_env(:stream_processing, :node)
      }
    )
    {:ok, %Data{message_queue: mq, serving_layer: serving_layer}}
  end

  def start(bp, sp) do
    {:ok, mq} = MessageQueue.start()
    {:ok, serving_layer} = ServingLayer.start(mq)
    BatchLayer.start(mq, bp)
    SpeedLayer.start(mq, sp)
    {:ok, %Data{message_queue: mq, serving_layer: serving_layer}}
  end

  def read_data(lambda_data, folder) do
    FrontendLayer.start(lambda_data.mq, folder)
  end

  def stop(lambda_data) do
    MessageQueue.add_message(lambda_data.mq, :speed_layer, :kill)
    MessageQueue.add_message(lambda_data.mq, :batch_layer, :kill)
    MessageQueue.add_message(lambda_data.mq, :serving_layer, :kill)
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
