defmodule BatchLayer.DataReader do
  @moduledoc """
  Data Reader for the BatchLayer Process.
  """

  @doc """
  Starts the Data Reader.
  """
  def start(bl, folder) do
    spawn(fn -> read_data(bl.batch_processing, folder) end)
  end

  # Reads the Data from the files
  defp read_data(bp, folder) do
    {:ok, files} = File.ls(folder)
    files = Enum.filter(files, fn file -> String.starts_with?(file, "stundenwerte_TU") end)
    read_data(bp, folder, files)
  end

  defp read_data(_bp, _folder, []) do
    IO.puts "All files finished"
  end

  defp read_data(bp, folder, [file | tail]) do
    f_name = folder <> file
    data = FileReader.read_file(f_name)
    BatchLayer.add_many(bp, data)
    read_data(bp, folder, tail)
  end

end
