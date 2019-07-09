defmodule MessageQueue.Test do

  def test do
    {:ok, server} = MessageQueue.start()
    MessageQueue.add_module(server, :test, :secret)
    MessageQueue.add_message(server, :test, :successfull)
    case MessageQueue.get_message(server, :test, :secret) do
      {:message, :successfull} ->
        IO.puts("First test solved")
      _any ->
        IO.puts("First test not solved")
    end
    case MessageQueue.get_message(server, :test, :not_secret) do
      {:error, :wrong_secret} ->
        IO.puts("Second test solved")
      _any ->
        IO.puts("Second test not solved")
    end
    case MessageQueue.add_module(server, :test, :secret) do
      {:error, :already_in} ->
        IO.puts("Third test solved")
      _any ->
        IO.puts("Third test not solved")
    end
    MessageQueue.stop(server)
  end

end
