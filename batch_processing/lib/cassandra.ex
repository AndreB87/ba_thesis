defmodule BatchProcessing.Cassandra do
  @moduledoc """
  Interface to the Cassandra Database needed for the Batch Processing Service.
  """

  @doc """
  Opens a connection to the database

  ## Examples

      iex> Cassandra.open()
      # => {:ok, connection}
  """
  def open(node) do
    Xandra.start_link(nodes: [node])
  end

  @doc """
  Gets the temperature data by `station`, `year`, `month`, `day` and `hour`
  """
  def get_temp_data_station(conn, station, year, month, day, hour) do
    statement = get_select_prefix() <> " WHERE station = ? AND year = ? AND month = ? AND day = ? AND hour = ?"
    arguments = [
      {"int", station},
      {"int", year},
      {"int", month},
      {"int", day},
      {"int", hour}
    ]
    execute(conn, statement, arguments)
  end

  @doc """
  Gets the temperature data by `station`, `year`, `month` and `day`
  """
  def get_temp_data_station(conn, station, year, month, day) do
    statement = get_select_prefix() <> " WHERE station = ? AND year = ? AND month = ? AND day = ?"
    arguments = [
      {"int", station},
      {"int", year},
      {"int", month},
      {"int", day}
    ]
    execute(conn, statement, arguments)
  end

  @doc """
  Gets the temperature data by `station`, `year` and `month`
  """
  def get_temp_data_station(conn, station, year, month) do
    statement = get_select_prefix() <> " WHERE station = ? AND year = ? AND month = ?"
    arguments = [
      {"int", station},
      {"int", year},
      {"int", month}
    ]
    execute(conn, statement, arguments)
  end

  @doc """
  Gets the temperature data by `station` and `year`
  """
  def get_temp_data_station(conn, station, year) do
    statement = get_select_prefix() <> " WHERE station = ? AND year = ?"
    arguments = [{"int", station}, {"int", year}]
    execute(conn, statement, arguments)
  end

  @doc """
  Gets the temperature data by `station` and `year`
  """
  def get_temp_data_station(conn, station) do
    statement = get_select_prefix() <> " WHERE station = ?"
    arguments = [{"int", station}]
    execute(conn, statement, arguments)
  end

  @doc """
  Gets the temperature data by `year`, `month`, `day` and `hour`
  """
  def get_temp_data(conn, year, month, day, hour) do
    statement = get_select_prefix() <> " WHERE year = ? AND month = ? AND day = ? AND hour = ?"
    arguments = [{"int", year}, {"int", month}, {"int", day}, {"int", hour}]
    execute(conn, statement, arguments)
  end

  @doc """
  Gets the temperature data by year, month and day
  """
  def get_temp_data(conn, year, month, day) do
    statement = get_select_prefix() <> " WHERE year = ? AND month = ? AND day = ?"
    arguments = [{"int", year}, {"int", month}, {"int", day}
    ]
    execute(conn, statement, arguments)
  end

  @doc """
  Gets the temperature data by year and month
  """
  def get_temp_data(conn, year, month) do
    statement = get_select_prefix() <> " WHERE year = ? AND month = ?"
    arguments = [{"int", year}, {"int", month}]
    execute(conn, statement, arguments)
  end

  @doc """
  Gets the temperature data by year
  """
  def get_temp_data(conn, year) do
    statement = get_select_prefix()
    arguments = [{"int", year}]
    execute(conn, statement, arguments)
  end

  @doc """
  Inserts temperature date to the database
  """
  def insert_temp_data(conn, data) do
    statement = "INSERT INTO temperatur.temp (station, year, month, day, hour, temp, hum) VALUES (?, ?, ?, ?, ?, ?, ?)"
    arguments = [
      {"int", Map.fetch!(data, "station")},
      {"int", Map.fetch!(data, "year")},
      {"int", Map.fetch!(data, "month")},
      {"int", Map.fetch!(data, "day")},
      {"int", Map.fetch!(data, "hour")},
      {"float", Map.fetch!(data, "temp")},
      {"float", Map.fetch!(data, "hum")}
    ]
    case Xandra.execute(conn, statement, arguments) do
      :error ->
        :error
      {:ok, _void} ->
        :ok
    end
  end

  @doc """
  Corrects temperature date to the database
  """
  def correct_temp_data(conn, data) do
    statement = "UPDATE temperatur.temp SET temp = ?, hum = ? WHERE station = ? AND year = ? AND month = ? AND day = ? AND hour = ?"
    arguments = [
      {"float", data.temp},
      {"float", data.hum},
      {"int", data.station},
      {"int", data.year},
      {"int", data.month},
      {"int", data.day},
      {"int", data.hour}
    ]
    case Xandra.execute(conn, statement, arguments) do
      :error ->
        :error
      {:ok, _void} ->
        :ok
    end
  end

  @doc """
  Deletes temperature date to the database
  """
  def delete_temp_data(conn, station, year, month, day, hour) do
    statement = "DELETE FROM temperatur.temp WHERE station = ? AND year = ? AND month = ? AND day = ? AND hour = ?"
    arguments = [
      {"int", station},
      {"int", year},
      {"int", month},
      {"int", day},
      {"int", hour}
    ]
    execute(conn, statement, arguments)
  end

  @doc """
  Returns the unique values from the `column` in the database
  """
  def get_unique_values(conn, column) do
    statement = "SELECT DISTINCT #{column} FROM temperatur.temp"
    execute(conn, statement, [])
  end

  # Executes the given query `statement` with the arguments
  defp execute(conn, statement, arguments) do
    case Xandra.execute(conn, statement, arguments, page_size: 1000000) do
      {:ok, result} ->
        {:ok, Enum.to_list(result)}
      any ->
        any
    end
  end

  # Returns the needed prefix for almost every select query
  defp get_select_prefix do
    "SELECT station, year, month, day, hour, temp, hum FROM temperatur.temp"
  end

end
