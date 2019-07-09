defmodule SpeedLayer do
  @moduledoc """
  Documentation for SpeedLayer.
  """

  alias SpeedLayer.Data, as: Data

  @doc """
  Starts the SpeedLayer.

  `serving_layer` -> Serving Layer of the Lambda Architecture
  """
  def start(serving_layer) do
    stream_processing = {
      Application.get_env(:stream_processing, :name),
      Application.get_env(:stream_processing, :node)
    }
    receive_pid = spawn(fn -> receive_loop(serving_layer) end)
    StreamProcessing.add_receiver(stream_processing, receive_pid)
    {:ok, %Data{stream_processing: stream_processing, server: receive_pid}}
  end

  def stop(sl_data) do
    send(sl_data.server, :kill)
    StreamProcessing.stop(sl_data.stream_processing)
  end

  @doc """
  Add a temp-data to the Stream Processing Processing

  `sl_data` -> ServingLayer-Data
  `station` -> Station from Temperature Data
  `year` -> Year from the Temperature Data
  `mont` -> Month from the Temperature Data
  `day` -> Day from the Temperature Data
  `hour` -> Hour from the Temperature Data
  `temp` -> Temperature from the Temperature Data
  `hum` -> Humidity from the Temperature Data
  """
  def add_data(sl_data, station, year, month, day, hour, temp, hum) do
    StreamProcessing.add_data(
      sl_data.stream_processing,
      %{
        "station" => station,
        "year" => year,
        "month" => month,
        "day" => day,
        "hour" => hour,
        "temp" => temp,
        "hum" => hum
      }
    )
  end

  @doc """
  Add a list of temp-data to the Stream Processing Processing

  `sl_data` -> ServingLayer-Data
  `data` -> List of Temperature Data
  """
  def add_many(sl_data, data) do
    StreamProcessing.add_many(sl_data.stream_processing, data)
  end

  # Loop to receive results from StreamProcessing Process
  #
  # serving_layer -> Serving Layer PID
  defp receive_loop(serving_layer) do
    receive do
      {:stream_processing, :result, results} ->
        send(serving_layer, {:speed_layer, :result, results})
        receive_loop(serving_layer)

      :kill ->
        :ok

      any ->
        IO.puts("Unrecognized Message: SpeedLayer")
        IO.puts(inspect(any))
        receive_loop(serving_layer)
    end
  end
end
