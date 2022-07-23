use rand::{self, thread_rng, Rng};

use csv::WriterBuilder;

// This doesn't generate perfect data but it's a decent fuzz tester to see if the
// code can handle odd situations. Depending on the speed of your random number generator,
// you may want to change the number of rows generated because it can take a _very_ long time.
#[test]
fn generate_test_data() {
    let memory_info = sys_info::mem_info().unwrap();
    // The file generate should be larger than the total memory of the system.
    let filesize = memory_info.total * 1024 + 1;
    let mut csv_writer = WriterBuilder::new()
        .delimiter(b',')
        .from_path("tests/large_input.csv")
        .unwrap();
    csv_writer
        .write_record(&["type", "client", "tx", "amount"])
        .unwrap();
    let mut done = false;
    let mut last_transaction_number: u32 = 1;
    let mut bytes_written = 0;
    let mut last_dispute_number: u32 = 0;
    let mut client_number: u16 = 1;
    // Try generating a single client that has random transaction values
    let mut rng = thread_rng();
    while !done {
        if last_transaction_number == u32::MAX {
            break;
        }
        if last_transaction_number % 1000 == 0 {
            client_number += 1;
        }
        // Generate unique random numbers between 0 and u16::MAX for client accounts
        let transaction_type = match rng.gen_range(0..=3) {
            0 => "deposit",
            1 => "withdrawal",
            2 => "dispute",
            3 => "resolve",
            _ => "deposit",
        };
        if transaction_type == "dispute" {
            // Dispute a previous charge
            last_dispute_number = last_transaction_number - 1;
        }
        let amount = rand::random::<f32>();
        let record = vec![
            transaction_type.to_string(),
            client_number.to_string(),
            if transaction_type == "dispute" || transaction_type == "resolve" {
                last_dispute_number.to_string()
            } else {
                last_transaction_number.to_string()
            },
            amount.to_string(),
        ];
        bytes_written += record.join(",").len();
        csv_writer.write_record(&record).unwrap();
        if bytes_written as u64 > filesize {
            done = true;
        } else {
            last_transaction_number += 1;
        }
    }
    // Generate unique random numbers between 0 and u16::MAX for client accounts
    // Generate a number of random transactions for each account
    // Write them out as a csv file
}
