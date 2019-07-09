defmodule BatchLayer do
  @moduledoc """
  Documentation for BatchLayer.
  """

  alias BatchLayer.Data, as: Data
  alias BatchLayer.MqConnection, as: MqConnection

  @doc """
  Starts the BatchLayer.

  `message_queue` -> PID or Tuple with Name and Node-Name of Message Queue
  `batch_processing` -> PID or Tuple with Name and Node-Name of Batch Processing Module
  """
  def start(message_queue, batch_processing) do
    #IO.puts("Starting BatchLayer")
    #IO.puts("-- Receive Loop")
    receive_pid = spawn(fn -> receive_loop(message_queue) end)
    #IO.puts("-- Add Receive Loop to BatchProcessing")
    BatchProcessing.add_receiver(batch_processing, receive_pid)
    bl_data = %Data{batch_processing: batch_processing}
    #IO.puts("-- Start Connection to Message Queue")
    MqConnection.start(message_queue, bl_data, :batch_layer, :batch_layer_secret)
    :ok
  end

  @doc """
  Stops the Batch Layer

  `bl_data` -> BatchLayer Data
  """
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
  def add_data(bl_data, data) do
    BatchProcessing.add_data(bl_data.batch_processing, data)
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
  def correct_data(bl_data, data) do
    BatchProcessing.correct_data(bl_data.batch_processing, data)
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
  defp receive_loop(mq) do
    receive do
      {:batch_process, :result, results} ->
        IO.puts("Send Results to Message Queue")
        MqConnection.send_results(mq, results)
        receive_loop(mq)

      :kill ->
        :ok

      any ->
        IO.puts("Unrecognized Message: BatchLayer")
        IO.puts(inspect(any))
        receive_loop(mq)
    end
  end

end
