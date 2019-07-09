defmodule FrameworkProvider.Database do
  @moduledoc """
  Documentation for BatchLayer.
  """

  alias FrameworkProvider.MessageModule, as: MessageModule

  @doc """
  Initializes the MnesiaServer.
  Must be called once per Node.
  """
  def init(mnesia_server, ram_only \\true) do
    MnesiaServer.init(mnesia_server)
    table = :analytics
    columns = [:id, :temp, :hum, :station, :year, :month, :day, :elements, :temp_elements, :hum_elements]
    indices = [:station, :year, :month, :day]
    create_table(mnesia_server, table, columns, indices, ram_only)
  end

  @doc """
  Starts the Database Module.

  `message_queue` -> PID or Tuple with Name and Node-Name of Message Queue
  `batch_processing` -> PID or Tuple with Name and Node-Name of Batch Processing Module
  """
  def start(message_module, mnesia_server, batch_processing, cas_id, cas_secret, mnesia_id, mnesia_secret) do
    MessageModule.add_module(message_module, cas_id, cas_secret)
    MessageModule.add_module(message_module, mnesia_id, mnesia_secret)
    mnesia_con = spawn(fn -> mnesia_loop(mnesia_server) end)
    spawn(fn -> recv_messages_cassandra(message_module, batch_processing, cas_id, cas_secret) end)
    spawn(fn -> recv_messages_mnesia(message_module, mnesia_con, mnesia_id, mnesia_secret) end)
    {:ok, mnesia_con}
  end

  @doc """
  Stops the Database Module

  `bl_data` -> BatchLayer Data
  """
  def stop(batch_processing) do
    BatchProcessing.stop(batch_processing)
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
  def add_data(batch_processing, station, year, month, day, hour, temp, hum) do
    BatchProcessing.add_data(
      batch_processing,
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
  def add_many(batch_processing, data) do
    BatchProcessing.add_many(batch_processing, data)
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
  def correct_data(batch_processing, station, year, month, day, hour, temp, hum) do
    BatchProcessing.correct_data(
      batch_processing,
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
  def delete_data(batch_processing, station, year, month, day, hour) do
    BatchProcessing.delete_data(batch_processing, station, year, month, day, hour)
  end

  defp add_result(mnesia_server, table, result) do
    Enum.each(Map.keys(result), fn key -> add_result(mnesia_server, table, key, Map.fetch!(result, key)) end)
  end

  defp add_result(mnesia_server, table, :avg_complete, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    year = 0
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, :avg_complete, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, result_key, result) do
    Enum.each(
      Map.keys(result),
      fn key ->
        [sub_result] = Map.fetch!(result, key);
        add_result(mnesia_server, table, result_key, key, sub_result)
      end
    )
  end

  defp add_result(mnesia_server, table, :avg_year, year, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, year, temp, hum, station, {:year, year}, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_month, {year, month}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:month, year, month}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_day, {year, month, day}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:day, year, month, day}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_station_complete, station, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    year = 0
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:station, station}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_station_year, {station, year}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:station, station, year}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_station_month, {station, year, month}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:station, station, year, month}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_station_day, {station, year, month, day}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:station, station, year, month, day}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, key, temp, hum, station, year, month, day, elements, temp_elements, hum_elements) do
    MnesiaServer.write(
      mnesia_server,
      table,
      [key, temp, hum, station, year, month, day, elements, temp_elements, hum_elements]
    )
  end

  defp get_result(mnesia_server, key, receiver) do
    result = get_single_result(mnesia_server, :analytics, key)
    send(receiver, {:result, result})
  end

  defp get_single_result(mnesia_server, table, key) do
    case MnesiaServer.read(mnesia_server, table, key) do
      {:error, error} ->
        {:error, error}
      {:ok, {_table, _key, avg_temp, avg_hum, station, year, month, day, elements, temp_elements, hum_elements}} ->
        %{
          avg_temp: avg_temp,
          avg_hum: avg_hum,
          elements: elements,
          temp_elements: temp_elements,
          hum_elements: hum_elements,
          station: station,
          year: year,
          month: month,
          day: day
        }
    end
  end

  defp create_table(mnesia_server, table, columns, indices, ram_only) do
    IO.puts inspect MnesiaServer.create_table(mnesia_server, table, columns, ram_only)
    Enum.each(indices, fn index -> MnesiaServer.create_index(mnesia_server, table, index) end)
  end

  # Loop to receive Messages from Messaging Module
  #
  # serving_layer -> Serving Layer PID or Tuple
  defp recv_messages_cassandra(message_module, batch_processing, id, secret) do
    case MessageModule.get_message(message_module, id, secret) do
      {:message, :no_message} ->
        recv_messages_cassandra(message_module, batch_processing, id, secret)

      {:message, {:preparation_module, :data, data}} ->
        IO.puts("Add Data")
        add_many(batch_processing, data)
        recv_messages_cassandra(message_module, batch_processing, id, secret)

      {:message, :kill} ->
        IO.puts("Kill")
        stop(batch_processing)

      any ->
        IO.puts("Unknown Message: Database Connection")
        IO.puts(inspect(any))
        recv_messages_cassandra(message_module, batch_processing, id, secret)
    end
  end

  # Loop to receive Messages from Messaging Module
  #
  # serving_layer -> Serving Layer PID or Tuple
  defp recv_messages_mnesia(message_module, mnesia_connection, id, secret) do
    case MessageModule.get_message(message_module, id, secret) do
      {:message, :no_message} ->
        recv_messages_mnesia(message_module, mnesia_connection, id, secret)

      {:message, {:analytics, :result, result}} ->
        IO.puts("Add Result")
        send(mnesia_connection, {:analytics, :result, result})
        recv_messages_mnesia(message_module, mnesia_connection, id, secret)

      {:message, :kill} ->
        IO.puts("Kill")
        send(mnesia_connection, :kill)

      any ->
        IO.puts("Unknown Message: Database Connection")
        IO.puts(inspect(any))
        recv_messages_mnesia(message_module, mnesia_connection, id, secret)
    end
  end

  defp mnesia_loop(mnesia_server) do
    receive do
      {:analytics, :result, result} ->
        spawn(fn -> add_result(mnesia_server, :analytics, result) end)
        mnesia_loop(mnesia_server)

      {:get, :result, key, receiver} ->
        spawn(fn -> get_result(mnesia_server, key, receiver) end)
        mnesia_loop(mnesia_server)

      :kill ->
        MnesiaServer.stop(mnesia_server)
        :ok

      any ->
        IO.puts("Unrecognized Message: MNesia Connection Receive Loop")
        IO.puts(inspect(any))
        mnesia_loop(mnesia_server)
    end
  end

end
