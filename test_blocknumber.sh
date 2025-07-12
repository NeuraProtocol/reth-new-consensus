
#!/bin/bash
for i in 5 6 7 8;
do
	curl -X POST -H 'Content-Type: application/json' --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["'$1'",false],"id":1}' http://localhost:854${i} -s | jq -r
done
