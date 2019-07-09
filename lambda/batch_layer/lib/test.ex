defmodule BatchLayer.Test do

  def test(db_host) do
    pid = spawn(fn -> test_loop() end)
    BatchLayer.start(db_host, pid)
  end

  defp test_loop() do
    receive do
      {:batch_layer, :results, result} ->
        IO.puts("vvv BatchLayer Result vvv")
        IO.puts(inspect(result))
        IO.puts("^^^ BatchLayer Result ^^^")
        test_loop()

      {:speed_layer, :results, result} ->
        IO.puts("vvv SpeedLayer Result vvv")
        IO.puts(inspect(result))
        IO.puts("^^^ SpeedLayer Result ^^^")
        test_loop()

      :kill ->
        :ok

      any ->
        IO.puts("vvv BatchLayer Result vvv")
        IO.puts(inspect(any))
        IO.puts("^^^ BatchLayer Result ^^^")
        test_loop()
    end
  end

end
