defmodule DataManagement do

  @moduledoc """
  Documentation for DataManagement.
  """

  alias DataManagement.ResultServer, as: ResultServer
  alias DataManagement.Data, as: Data

  @doc """
  Initialises the Serving Layer and the MNesia Database.

  `ram_only` -> `true` -> Results are saved in RAM only (not persistent)
                `false` -> Results are saved on harddisk (persistent)
  """
  def init(ram_only \\false) do
     ResultServer.init(ram_only)
  end

  @doc """
  Starts the DataManagement.

  `serving_layer` -> Serving Layer PID or Tuple with Name and Node-Name of Serving Layer

  ## Examples

    iex> BatchLayer.init("127.0.0.1:9042", {:serving_layer, :serving_layer@serving_node})
    # {:ok, BatchLayerData}

  """
  def start() do
    batch_processing = {
      Application.get_env(:batch_processing, :name),
      Application.get_env(:batch_processing, :node)
    }
    {:ok, result_server} = ResultServer.start()
    data = %Data{batch_processing: batch_processing, result_server: result_server}
    receive_pid = spawn(fn -> receive_loop(data) end)
    BatchProcessing.add_receiver(batch_processing, receive_pid)
    {:ok, data}
  end

  @doc """
  Stops the Data Management Service
  """
  def stop(dm_data) do
    BatchProcessing.stop(dm_data.batch_processing)
    ResultServer.stop(dm_data.result_server)
    :ok
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
  def add_data(dm_data, station, year, month, day, hour, temp, hum) do
    BatchProcessing.add_data(
      dm_data.batch_processing,
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
  def add_many(dm_data, data) do
    BatchProcessing.add_many(dm_data.batch_processing, data)
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
  def correct_data(dm_data, station, year, month, day, hour, temp, hum) do
    BatchProcessing.correct_data(
      dm_data.batch_processing,
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
  def delete_data(dm_data, station, year, month, day, hour) do
    BatchProcessing.delete_data(dm_data.batch_processing, station, year, month, day, hour)
  end

  @doc """
  Returns the average temperature and humidity for the `year`.
  """
  def get_result(dm_data, key) do
    ResultServer.get_result(dm_data.result_server, key)
  end

  # Loop to receive Results from BatchProcessing Process
  #
  # serving_layer -> Serving Layer PID or Tuple
  defp receive_loop(dm_data) do
    receive do
      {:batch_process, :result, results} ->
        spawn(fn -> ResultServer.add_result(dm_data.result_server, results) end)
        receive_loop(dm_data)
      :kill ->
        :ok
      any ->
        IO.puts("Unrecognized Message: DataManagement")
        IO.puts(inspect(any))
        receive_loop(dm_data)
    end
  end

end
