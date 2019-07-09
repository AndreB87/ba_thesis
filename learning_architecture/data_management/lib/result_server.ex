defmodule DataManagement.ResultServer do
  @moduledoc """
  Result Server.
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
    columns = [:id, :temp, :hum, :station, :year, :month, :day, :elements, :temp_elements, :hum_elements]
    indices = [:station, :year, :month, :day]
    create_table(mnesia_server, :batch_result, columns, indices, ram_only)
  end

  @doc """
  Starts the MnesiaServer for the Serving Layer.

  ## Examples

      iex> ServingLayer.start()
      # {:ok, ServingLayerPID}

  """
  def start do
    MnesiaServer.start()
    mnesia_server = {
      Application.get_env(:mnesia_server, :name),
      Application.get_env(:mnesia_server, :node)
    }
    {:ok, mnesia_server}
  end

  @doc """
  Stops the MnesiaServer
  """
  def stop(mnesia_server) do
    MnesiaServer.stop(mnesia_server)
  end

  @doc """
  Adds a result to the Mnesia database
  """
  def add_result(mnesia_server, result) do
    Enum.each(Map.keys(result), fn key -> add_result(mnesia_server, key, Map.fetch!(result, key)) end)
  end

  @doc """
  Gets a result from the Mnesia database

  `mnesia_server` is the PID for the mnesia database Connection
  `key` is the key, the result is stored with
  """
  def get_result(mnesia_server, key) do
    {:result, get_single_result(mnesia_server, :batch_result, key)}
  end

  defp add_result(mnesia_server, :avg_complete, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    year = 0
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, :avg_complete, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, result_key, result) do
    Enum.each(
      Map.keys(result),
      fn key ->
        [sub_result] = Map.fetch!(result, key);
        add_result(mnesia_server, result_key, key, sub_result)
      end
    )
  end

  defp add_result(mnesia_server, :avg_year, year, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, year, temp, hum, station, {:year, year}, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, :avg_month, {year, month}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, {:month, year, month}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, :avg_day, {year, month, day}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, {:day, year, month, day}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, :avg_station_complete, station, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    year = 0
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, {:station, station}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, :avg_station_year, {station, year}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, {:station, station, year}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, :avg_station_month, {station, year, month}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, {:station, station, year, month}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, :avg_station_day, {station, year, month, day}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, {:station, station, year, month, day}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, key, temp, hum, station, year, month, day, elements, temp_elements, hum_elements) do
    MnesiaServer.write(
      mnesia_server,
      :batch_result,
      [key, temp, hum, station, year, month, day, elements, temp_elements, hum_elements]
    )
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
