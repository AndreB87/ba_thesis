defmodule SpeedLayer.DataReader do
  @moduledoc """
  Documentation for DataReader.
  """

  @doc """
  Starts the Data Reader for the Speed Layer.
  """
  def start(sl, folder) do
    spawn(fn -> read_data(sl, folder) end)
  end

  # Read in the Data from files
  #
  # sp -> Process ID of Stream Processing unit
  defp read_data(sl, folder) do
    {:ok, files} = File.ls(folder)
    files = Enum.filter(files, fn file -> String.starts_with?(file, "stundenwerte_TU") end)
    read_data(sl, folder, files)
  end

  defp read_data(_sl, _folder, []) do
    IO.puts "All files finished"
  end

  defp read_data(sl, folder, [file | tail]) do
    f_name = folder <> file
    data = FileReader.read_file(f_name)
    SpeedLayer.add_many(sl, data)
    read_data(sl, folder, tail)
  end

end
