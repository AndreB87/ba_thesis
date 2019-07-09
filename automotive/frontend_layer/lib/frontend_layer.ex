defmodule FrontendLayer do
  @moduledoc """
  Frontend Layer for the Big Data Architecture for Automotive Applications.
  """

  @doc """
  Starts the FrointendLayer.
  """
  def start(message_queue, folder) do
    spawn(fn -> read_data(message_queue, folder) end)
  end

  # Reads the Data from the files
  #
  # message_queue -> Connection to the MessageQueue
  # folder -> Folder containing the files to read in
  defp read_data(message_queue, folder) do
    {:ok, files} = File.ls(folder)
    files = Enum.filter(files, fn file -> String.starts_with?(file, "stundenwerte_TU") end)
    read_data(message_queue, folder, files)
  end

  defp read_data(_bp, _folder, []) do
    IO.puts "All files finished"
  end

  defp read_data(message_queue, folder, [file | tail]) do
    f_name = folder <> file
    MessageQueue.add_message(message_queue, :speed_layer, {:add, :many, read_file(f_name)})
    read_data(message_queue, folder, tail)
  end

  # Reads in the file with the given name
  #
  # fliename -> Name of the file to read
  defp read_file(filename) do
    filename = to_charlist(filename)
    case :zip.unzip(filename, [:memory]) do
      {:ok, content} ->
        {_name, file} = Enum.fetch!(content, -1)
        split_file(file)
      any ->
        any
    end
  end

  # Split file into rows
  defp split_file file do
    [_header | lines] = String.split(to_string(file), ";eor\r\n")
    lines
  end

end
