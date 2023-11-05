MODE=release

bins:
	cargo build --$(MODE)

echo: bins
	maelstrom/maelstrom test -w echo --bin target/$(MODE)/echo --node-count 1 --time-limit 10 --log-stderr

unique-ids: bins
	maelstrom/maelstrom test -w unique-ids --bin target/$(MODE)/unique_ids --node-count 3 --time-limit 30 --rate 1000 --availability total --nemesis partition --log-stderr

broadcast: bins
	maelstrom/maelstrom test -w broadcast --bin target/$(MODE)/broadcast --node-count 1 --time-limit 20 --rate 10

broadcast-multi: bins
	maelstrom/maelstrom test -w broadcast --bin target/$(MODE)/broadcast-multi --node-count 5 --time-limit 20 --rate 10

broadcast-fault-tolerant: bins
	maelstrom/maelstrom test -w broadcast --bin target/$(MODE)/broadcast-fault-tolerant --node-count 5 --time-limit 20 --rate 10 --nemesis partition

broadcast-efficiency-one: bins
	maelstrom/maelstrom test -w broadcast --bin target/$(MODE)/broadcast-efficiency-one --node-count 25 --time-limit 20 --rate 100 --latency 100

broadcast-efficiency-one-partitions: bins
	maelstrom/maelstrom test -w broadcast --bin target/$(MODE)/broadcast-efficiency-one --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition

broadcast-efficiency-two: bins
	maelstrom/maelstrom test -w broadcast --bin target/$(MODE)/broadcast-efficiency-two --node-count 25 --time-limit 20 --rate 100 --latency 100
