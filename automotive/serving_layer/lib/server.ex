defmodule ServingLayer.Server do
  @moduledoc """
  Server for the ServingLayer.
  """

  @doc """
  Initializes the MnesiaServer used for the ServingLayer Server.
  Must be called once per Node.
  """
  def init(ram_only \\false) do
    mnesia_server = {
      Application.get_env(:mnesia_server, :name),
      Application.get_env(:mnesia_server, :node)
    }
    MnesiaServer.init(mnesia_server)
    tables = [:speed_result, :batch_result]
    columns = [:id, :temp, :hum, :station, :year, :month, :day, :elements, :temp_elements, :hum_elements]
    indices = [:station, :year, :month, :day]
    Enum.each(tables, fn table -> create_table(mnesia_server, table, columns, indices, ram_only) end)
  end

  @doc """
  Starts the MnesiaServer for the Serving Layer.

  ## Examples

      iex> ServingLayer.start()
      # {:ok, ServingLayerPID}

  """
  def start do
    mnesia_server = {
      Application.get_env(:mnesia_server, :name),
      Application.get_env(:mnesia_server, :node)
    }
    {:ok, spawn(fn -> receive_loop(mnesia_server) end)}
  end

  defp receive_loop(mnesia_server) do
    receive do
      {:speed_layer, :result, result} ->
        spawn(fn -> add_result(mnesia_server, :speed_result, result) end)
        receive_loop(mnesia_server)
      {:batch_layer, :result, result} ->
        spawn(fn -> add_result(mnesia_server, :batch_result, result) end)
        receive_loop(mnesia_server)
      {:get, :result, key, receiver} ->
        spawn(fn -> get_result(mnesia_server, key, receiver) end)
        receive_loop(mnesia_server)
      :kill ->
        MnesiaServer.stop(mnesia_server)
        :ok
      any ->
        IO.puts("Unrecognized Message: ServingLayer")
        IO.puts(inspect(any))
        receive_loop(mnesia_server)
    end
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

  defp add_result(mnesia_server, :speed_result, result_key, result) do
    Enum.each(
      Map.keys(result),
      fn key ->
        add_result(mnesia_server, :speed_result, result_key, key, Map.fetch!(result, key))
      end
    )
  end

  defp add_result(mnesia_server, :batch_result, result_key, result) do
    Enum.each(
      Map.keys(result),
      fn key ->
        [sub_result] = Map.fetch!(result, key);
        add_result(mnesia_server, :batch_result, result_key, key, sub_result)
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
    case get_single_result(mnesia_server, :batch_result, key) do
      {:error, _error} ->
        send(receiver, {:result, {:speed_layer, MnesiaServer.read(mnesia_server, :speed_result, key)}})
      batch_result ->
        case get_single_result(mnesia_server, :speed_result, key) do
          {:error, :not_found} ->
            send(receiver, {:result, {:batch_layer, batch_result}})
          speed_result ->
            if speed_result.elements > batch_result.elements do
              send(receiver, {:result, {:speed_layer, speed_result}})
            else
              send(receiver, {:result, {:batch_layer, batch_result}})
            end
        end
    end
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
    MnesiaServer.create_table(mnesia_server, table, columns, ram_only)
    Enum.each(indices, fn index -> MnesiaServer.create_index(mnesia_server, table, index) end)
  end

end
