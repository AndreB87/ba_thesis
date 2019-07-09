defmodule SpeedLayer.Test do

  def test() do
    {:ok, mq} = MessageQueue.start()
    MessageQueue.add_module(mq, :serving_layer, :secret)
    FrontendLayer.start(mq, "../../../Daten/")
    spawn(fn -> test_loop(mq, :serving_layer, :secret) end)
    {:ok, bp} = StreamProcessing.start()
    SpeedLayer.start(mq, bp)
  end

  defp test_loop(mq, id, secret) do
    case MessageQueue.get_message(mq, id, secret) do
      {:message, :no_message} ->
        :ok

      {:message, {:speed_layer, :result, result}} ->
        IO.puts("Received SpeedLayer Result :")
        IO.puts(inspect(result))

      any ->
        IO.puts("Received Message:")
        IO.puts(inspect(any))

    end
    test_loop(mq, id, secret)
  end

end
