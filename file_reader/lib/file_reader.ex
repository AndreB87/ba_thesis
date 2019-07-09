defmodule FileReader do
  @moduledoc """
  Module to read in a temperature file.
  """

  @doc """
  Reads a file and make a list of TempData out of it.
  """
  def read_file filename do
    filename = to_charlist(filename)
    case :zip.unzip(filename, [:memory]) do
      {:ok, content} ->
        {_name, file} = Enum.fetch!(content, -1)
        split_file file
      any ->
        any
    end
  end

  # Split file into rows
  defp split_file file do
    [_header | lines] = String.split(to_string(file), ";eor\r\n")
    Enum.map(lines, fn line -> line_to_temp_data line end)
    |> Enum.filter(fn line -> line != :eof end)
  end

  # Convert line to temp-data map
  defp line_to_temp_data line do
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
      [""] -> :eof
    end
  end

end
