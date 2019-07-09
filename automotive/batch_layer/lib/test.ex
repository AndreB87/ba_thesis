defmodule BatchLayer.Test do

  def test do
    {:ok, mq} = MessageQueue.start()
    id = :serving_layer
    secret = :secret
    MessageQueue.add_module(mq, id, secret)
    spawn(fn -> test_loop(mq, id, secret) end)
    {:ok, bp} = BatchProcessing.start()
    BatchLayer.start(mq, bp)
  end

  defp test_loop(mq, id, secret) do
    case MessageQueue.get_message(mq, id, secret) do
      {:message, :no_message} ->
        :ok

      {:message, {:batch_layer, :result, result}} ->
        IO.puts("Received BatchLayer Result :")
        IO.puts(inspect(result))

      any ->
        IO.puts("Received Message:")
        IO.puts(inspect(any))

    end
    test_loop(mq, id, secret)
  end

end
