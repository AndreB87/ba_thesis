defmodule DataGathering do
  @moduledoc """
  Data Gathering Device for the Learning Analytics Architecture.
  """

  @doc """
  Starts the Data Gathering Device.
  """
  def start(data_management, folder) do
    spawn(fn -> read_data(data_management, folder) end)
  end

  # Reads the Data from the files
  defp read_data(data_management, folder) do
    {:ok, files} = File.ls(folder)
    files = Enum.filter(files, fn file -> String.starts_with?(file, "stundenwerte_TU") end)
    read_data(data_management, folder, files)
  end

  defp read_data(_data_management, _folder, []) do
    IO.puts "All files finished"
  end

  defp read_data(data_management, folder, [file | tail]) do
    f_name = folder <> file
    data = FileReader.read_file(f_name)
    DataManagement.add_many(data_management, data)
    read_data(data_management, folder, tail)
  end
end
