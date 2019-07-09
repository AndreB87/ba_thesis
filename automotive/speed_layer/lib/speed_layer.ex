defmodule SpeedLayer do
  @moduledoc """
  Documentation for SpeedLayer.
  """

  alias SpeedLayer.Data, as: Data
  alias SpeedLayer.MqConnection, as: MqConnection

  @doc """
  Starts the SpeedLayer.

  `message_queue` -> PID or Tuple with Name and Node-Name of Message Queue
  `batch_processing` -> PID or Tuple with Name and Node-Name of Batch Processing Module
  """
  def start(message_queue, stream_processing) do
    receive_pid = spawn(fn -> receive_loop(message_queue) end)
    StreamProcessing.add_receiver(stream_processing, receive_pid)
    sl_data = %Data{stream_processing: stream_processing, server: receive_pid}
    MqConnection.start(message_queue, sl_data, :speed_layer, :speed_layer_secret)
    :ok
  end

  @doc """
  Stops the Speed Layer

  `sl_data` -> SpeedLayer Data
  """
  def stop(sl_data) do
    send(sl_data.server, :kill)
    StreamProcessing.stop(sl_data.stream_processing)
  end

  @doc """
  Add temp-data to the Stream Processing Processing

  `sl_data` -> ServingLayer-Data
  `mq` -> MessageQueue Connection
  `line` -> Temperature Data as Line
  """
  def add_line(sl_data, mq, line) do
    case preprocess_data(line) do
      :eof ->
        :eof

      data ->
        MqConnection.send_to_batch_layer(mq, data)
        add_data(sl_data, data)
    end
  end

  @doc """
  Add a list of temp-data to the Stream Processing Processing

  `sl_data` -> ServingLayer-Data
  `mq` -> MessageQueue Connection
  `data` -> List of Temperature Data as Line
  """
  def add_many(sl_data, mq, lines) do
    Enum.each(lines, fn line -> add_line(sl_data, mq, line) end)
  end

  # Add a temp-data to the Stream Processing Processing

  # sl_data -> ServingLayer-Data
  # data -> Preprocessed Temperature Data
  defp add_data(sl_data, data) do
    StreamProcessing.add_data(sl_data.stream_processing, data)
  end

  defp preprocess_data(line) do
    case String.split(line, ";") do
      [station, date, _quality_level, temp, hum] ->
        date = String.trim(date)
        {year, _} = Integer.parse(String.slice(date, 0..3))
        {month, _} = Integer.parse(String.slice(date, 4..5))
        {day, _} = Integer.parse(String.slice(date, 6..7))
        {hour, _} = Integer.parse(String.slice(date, 8..9))
        {station, _} = Integer.parse(String.trim(station))
        {temp, _} = Float.parse(String.trim(temp))
        {hum, _} = Float.parse(String.trim(hum))
        %{
          "station" => station,
          "year" => year,
          "month" => month,
          "day" => day,
          "hour" => hour,
          "temp" => temp,
          "hum" => hum
        }

      [""] ->
        :eof
    end
  end

  # Loop to receive results from StreamProcessing Process
  #
  # serving_layer -> Serving Layer PID
  defp receive_loop(mq_con) do
    receive do
      {:stream_processing, :result, results} ->
        MqConnection.send_results(mq_con, results)
        receive_loop(mq_con)

      :kill ->
        :ok

      any ->
        IO.puts("Unrecognized Message: SpeedLayer")
        IO.puts(inspect(any))
        receive_loop(mq_con)
    end
  end
end
