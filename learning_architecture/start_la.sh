path=$(dirname $0)
cd $path

elixir --sname mnesia -S mix run -e "MnesiaServer.start()" --no-halt &
elixir --sname batch -S mix run -e "BatchProcessing.start()" --no-halt &
iex --sname learning_architecture -S mix 
