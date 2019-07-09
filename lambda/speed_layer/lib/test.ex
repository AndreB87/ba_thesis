defmodule SpeedLayer.Test do

  def test() do
    pid = spawn(fn -> test_loop() end)
    {:ok, sl} = SpeedLayer.start(pid)
    SpeedLayer.DataReader.start(sl, "../../Daten")
  end

  defp test_loop() do
    receive do
      {:batch_layer, :result, result} ->
        IO.puts("vvv BatchLayer Result vvv")
        IO.puts(inspect(result))
        IO.puts("^^^ BatchLayer Result ^^^")
        test_loop()

      {:speed_layer, :result, result} ->
        IO.puts("vvv SpeedLayer Result vvv")
        IO.puts(inspect(result))
        IO.puts("^^^ SpeedLayer Result ^^^")
        test_loop()

      :kill ->
        :ok

      any ->
        IO.puts("vvv Unrecognized vvv")
        IO.puts(inspect(any))
        IO.puts("^^^ Unrecognized ^^^")
        test_loop()
    end
  end

end
