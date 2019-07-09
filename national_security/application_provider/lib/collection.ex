defmodule ApplicationProvider.Collection do
  @moduledoc """
  Read in the data and send it to the preparation module
  """

  alias FrameworkProvider.MessageModule, as: MessageModule

  @doc """
  Starts the Collection Module.
  """
  def start(message_module, receiver, folder) do
    spawn(fn -> read_data(message_module, receiver, folder) end)
  end

  # Reads the Data from the files
  defp read_data(message_module, receiver, folder) do
    {:ok, files} = File.ls(folder)
    files = Enum.filter(files, fn file -> String.starts_with?(file, "stundenwerte_TU") end)
    read_data(message_module, receiver, folder, files)
  end

  defp read_data(_message_module, _receiver, _folder, []) do
    IO.puts("All files finished")
  end

  defp read_data(message_module, receiver, folder, [file | tail]) do
    f_name = folder <> file
    data = read_file(f_name)
    message = {:add, :many, data}
    MessageModule.add_message(message_module, receiver, message)
    read_data(message_module, receiver, folder, tail)
  end

  def read_file(filename) do
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
  defp split_file(file) do
    [_header | lines] = String.split(to_string(file), ";eor\r\n")
    lines
  end

end
