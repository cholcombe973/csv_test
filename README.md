# CSV Transaction Processor

This is a simple CSV transaction processor. It takes a CSV file with the following fields: 
`type, client, tx, amount`. 
It will check the system it's running on to see if the csv file will fit into working memory. If it does then it will process the entire file using a HashMap. 
If it can't then it spins up a sled database and does a 2 pass process. 

It is possible that it can process files larger than working memory in memory but it makes a conservative assumption and will fall back.
There are several possible approaches to handling large CSV files. 
1. Use a LSM tree to store the transactions and a in memory hash table to store the client accounts.
2. Process the CSV file twice. The first time to process as many transactions as possible and the second time to process diputes, chargebacks and resolutions.
3. Use a database to store the transactions and an in memory hash table to store the client accounts.

In the interest of time, I have chosen the third approach. Tradeoffs such as memory usage vs execution time were considered.

The processor makes a best attempt to process as many transactions as possible. If an error occurs it will print it to stderr, set the approiate exit value and continue processing. 

## Architecture Decisions:
1. Sled was chosen for the database due to previous experience using RocksDB and various flavors of SQL. Sled has a nice combination of LSM like performance with a simple API. It also features built in compression, lock free operation and thread safety. It seemed like a good fit for this project.
2. A bit array was chosen for the client account tracker. The fixed size greatly improves the efficiency of storing which client ID's have been seen.

## TODO still: 
1. Handle scenarios where the client account is overdrawn. Currently it allows the account value to go negative.

## Possible Improvements:
1. The account processing could be merged with a trait possibly so that it doesn't go down different paths for in memory vs sled.
2. More profiling should be done to determine the best way to handle large files. Initial profiling done on 50-100MB files shows that the CPU is maxing out the single thread it's running in. There's also a data size amplification when using sled as the backing store. I think there's more efficiency to be wrung out of this project with some memory profiling.
3. Rayon could be used to parallelize the processing.
4. The memory info tester may not work properly in container environments, bare metal or virtual machines are suggested.

## Test Generation
If you would like to generate a test file for yourself please run `cargo test generate_test_data`. Please check the code first and adjust the amount of data to generate.
