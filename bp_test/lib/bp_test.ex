defmodule BpTest do

  def start_bp() do
    {:ok, bp} = BatchProcessing.init("172.17.0.2:9042")
    # spawn fn -> file_reader bp end
    receiver = spawn fn -> result_receiver() end
    BatchProcessing.add_receiver(bp, receiver)
  end

  def start_sp() do
    {:ok, sp} = StreamProcessing.init()
    receiver = spawn fn -> result_receiver() end
    StreamProcessing.add_receiver(sp, receiver)
    spawn fn -> file_reader_sp sp end
  end

  defp file_reader_one() do
    folder = "../../Daten/"
    {:ok, files} = File.ls(folder)
    files = Enum.filter(files, fn file -> String.starts_with?(file, "stundenwerte_TU") end)
    file_reader_one(folder, files)
  end

  defp file_reader_one(folder, [file | _tail]) do
    f_name = folder <> file
    IO.puts "Read #{f_name}"
    FileReader.read_file(f_name)
  end

  def file_reader(bp) do
    folder = "../../Daten/"
    {:ok, files} = File.ls(folder)
    files = Enum.filter(files, fn file -> String.starts_with?(file, "stundenwerte_TU") end)
    IO.puts(inspect(files))
    file_reader(bp, folder, files)
  end

  defp file_reader(_bp, _folder, []) do
    IO.puts "All files finished"
  end

  defp file_reader(bp, folder, [file | tail]) do
    f_name = folder <> file
    IO.puts "Read #{f_name}"
    data = FileReader.read_file(f_name)
    BatchProcessing.add_many(bp, data)
    file_reader(bp, folder, tail)
  end

  def file_reader_sp(sp) do
    folder = "../../Daten/"
    {:ok, files} = File.ls(folder)
    files = Enum.filter(files, fn file -> String.starts_with?(file, "stundenwerte_TU") end)
    file_reader_sp(sp, folder, files)
  end

  defp file_reader_sp(_sp, _folder, []) do
    IO.puts "All files finished"
  end

  defp file_reader_sp(sp, folder, [file | tail]) do
    f_name = folder <> file
    IO.puts "Read #{f_name}"
    data = FileReader.read_file(f_name)
    StreamProcessing.add_many(sp, data)
    file_reader_sp(sp, folder, tail)
  end

  defp result_receiver() do
    receive do
      :kill ->
        :ok

      {:stream_processing, :result, result} ->
        IO.puts("Stream Result incomming")
        IO.puts(inspect(result.avg_station_complete))
        result_receiver()

      any ->
        IO.puts("Result incomming")
        IO.puts(inspect(any))
        result_receiver()
    end
  end

end
