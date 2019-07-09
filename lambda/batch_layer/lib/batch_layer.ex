defmodule BatchLayer do
  @moduledoc """
  Documentation for BatchLayer.
  """

  alias BatchLayer.Data, as: Data

  @doc """
  Starts the BatchLayer.

  `db_host` -> IP Adress and Port of Cassandra Database as a String
  `serving_layer` -> Serving Layer PID or Tuple with Name and Node-Name of Serving Layer

  ## Examples

    iex> BatchLayer.init("127.0.0.1:9042", {:serving_layer, :serving_layer@serving_node})
    # {:ok, BatchLayerData}

  """
  def start(serving_layer) do
    batch_processing = {
      Application.get_env(:batch_processing, :name),
      Application.get_env(:batch_processing, :node)
    }
    receive_pid = spawn(fn -> receive_loop(serving_layer) end)
    BatchProcessing.add_receiver(batch_processing, receive_pid)
    {:ok, %Data{batch_processing: batch_processing}}
  end

  def stop(bl_data) do
    BatchProcessing.stop(bl_data.batch_processing)
  end

  @doc """
  Adds Temperature Data to the BatchLayer

  `bl_data` -> BatchLayer Connection Data
  `station` -> Station of the Temperature Data
  `year` -> Year of the Temperature Data
  `month` -> Month of the Temperature Data
  `day` -> Day of the Temperature Data
  `hour` -> Hour of the Temperature Data
  `temp` -> Temperature of the Temperature Data
  `hum` -> Humidity of the Temperature Data
  """
  def add_data(bl_data, station, year, month, day, hour, temp, hum) do
    BatchProcessing.add_data(
      bl_data.batch_processing,
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
  Adds a List of Temperature Data to the BatchLayer

  `bl_data` -> BatchLayer Connection Data
  `data` -> List of Temperature Data
  """
  def add_many(bl_data, data) do
    BatchProcessing.add_many(bl_data.batch_processing, data)
  end

  @doc """
  Corrects Temperature Data to the BatchLayer

  `bl_data` -> BatchLayer Connection Data
  `station` -> Station of the Temperature Data
  `year` -> Year of the Temperature Data
  `month` -> Month of the Temperature Data
  `day` -> Day of the Temperature Data
  `hour` -> Hour of the Temperature Data
  `temp` -> Temperature of the Temperature Data
  `hum` -> Humidity of the Temperature Data
  """
  def correct_data(bl_data, station, year, month, day, hour, temp, hum) do
    BatchProcessing.correct_data(
      bl_data.batch_processing,
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
  Deletes Temperature Data to the BatchLayer

  `bl_data` -> BatchLayer Connection Data
  `station` -> Station of the Temperature Data
  `year` -> Year of the Temperature Data
  `month` -> Month of the Temperature Data
  `day` -> Day of the Temperature Data
  `hour` -> Hour of the Temperature Data
  """
  def delete_data(bl_data, station, year, month, day, hour) do
    BatchProcessing.delete_data(bl_data.batch_processing, station, year, month, day, hour)
  end

  # Loop to receive Results from BatchProcessing Process
  #
  # serving_layer -> Serving Layer PID or Tuple
  defp receive_loop(serving_layer) do
    receive do
      {:batch_process, :result, results} ->
        send(serving_layer, {:batch_layer, :result, results})
        receive_loop(serving_layer)
      :kill ->
        :ok
      any ->
        IO.puts("Unrecognized Message: BatchLayer")
        IO.puts(inspect(any))
        receive_loop(serving_layer)
    end
  end

end
