/*
    Given a CSV representing a series of transactions, implement a simple toy transactions engine
    that processes the payments crediting and debiting accounts. After processing the complete set
    of payments output the client account balances

    Assumptions:
    1. The client has a single asset account. All transactions are to and from this single asset account;
    2. There are multiple clients. Transactions reference clients. If a client doesn't exist create a
    new record;
    3. Clients are represented by u16 integers. No names, addresses, or complex client profile
    info;

    // With a u32 transaction id the maximum number of transactions is 2^32 - 1 = 4,294,967,295

    what if i do a hybrid and consider disputes, chargebacks and resolutions to be not typical and
    only store those in the transaction logs if they occur. If that happens, mark the account
    as being in dispute and replay the CSV file to grab all the transaction id's that are
    required to resolve the disputes.

    If an account is marked as in dispute, when the csv file is replayed stored the entire
    transaction history for that client and then reconsile the account after it's finished reading.
    That way we only store a subset of the transactions that are needed to resolve the dispute.
*/

use anyhow::{anyhow, Result};
use bitvec::prelude as bv;
use csv::{ReaderBuilder, Trim};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, fmt, mem, path::Path};

// Serde deserialization helper
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

// Transaction represents a single transaction record in the CSV file.
#[derive(Clone, Debug, Deserialize)]
struct RawRecord {
    #[serde(rename = "type")]
    transaction_type: TransactionType,
    client: u16, // unique client id
    #[serde(rename = "tx")]
    transaction: u32, // globally unique transaction id
    amount: f32, // amount of money with up to 4 decimal places
}

// A single transaction that holds the transaction id and the amount of money
// that was transferred depending on the transaction type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum TransactionOp {
    Deposit(u32, f32),
    Withdraw(u32, f32),
    Dispute(u32),    // transaction id
    Resolve(u32),    // transaction_id
    Chargeback(u32), // transaction_id
}

impl From<RawRecord> for TransactionOp {
    fn from(record: RawRecord) -> Self {
        match record.transaction_type {
            TransactionType::Deposit => TransactionOp::Deposit(record.transaction, record.amount),
            TransactionType::Withdrawal => {
                TransactionOp::Withdraw(record.transaction, record.amount)
            }
            TransactionType::Dispute => TransactionOp::Dispute(record.transaction),
            TransactionType::Resolve => TransactionOp::Resolve(record.transaction),
            TransactionType::Chargeback => TransactionOp::Chargeback(record.transaction),
        }
    }
}

impl From<&RawRecord> for TransactionOp {
    fn from(record: &RawRecord) -> Self {
        match record.transaction_type {
            TransactionType::Deposit => TransactionOp::Deposit(record.transaction, record.amount),
            TransactionType::Withdrawal => {
                TransactionOp::Withdraw(record.transaction, record.amount)
            }
            TransactionType::Dispute => TransactionOp::Dispute(record.transaction),
            TransactionType::Resolve => TransactionOp::Resolve(record.transaction),
            TransactionType::Chargeback => TransactionOp::Chargeback(record.transaction),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Account {
    id: u16,
    available_funds: f32,
    held_funds: f32,
    total_funds: f32,
    locked: bool,
    in_dispute: bool, // if true, the account is in dispute and the CSV file needs to be replayed to resolve it
    last_processed_transaction: u32,
    transaction_log: Vec<TransactionOp>, // Either all transactions or a subset of transactions of one_pass is false
}

impl fmt::Display for Account {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}, {}, {}, {}, {}",
            self.id, self.available_funds, self.held_funds, self.total_funds, self.locked
        )
    }
}

#[test]
fn test_record_processor() {
    //
}

#[test]
fn test_process_account() {
    // Create some sample account transactions and process them

    // First test an easy case
    let mut account = Account {
        id: 1,
        available_funds: 0.0,
        held_funds: 0.0,
        total_funds: 0.0,
        locked: false,
        in_dispute: false,
        last_processed_transaction: 0,
        transaction_log: vec![
            TransactionOp::Deposit(1, 100.0000),
            TransactionOp::Withdraw(2, 50.0000),
            TransactionOp::Withdraw(3, 25.0000),
        ],
    };
    process_account(&mut account);
    assert_eq!(account.available_funds, 25.0000);
    assert_eq!(account.held_funds, 0.0);
    assert_eq!(account.total_funds, 25.0000);
    assert_eq!(account.locked, false);
    assert_eq!(account.in_dispute, false);
    assert_eq!(account.last_processed_transaction, 3);
    assert_eq!(account.transaction_log.len(), 0);
}

#[test]
fn test_process_dispute() {
    // Now test a more complex case
    let mut account = Account {
        id: 1,
        available_funds: 0.0,
        held_funds: 0.0,
        total_funds: 0.0,
        locked: false,
        in_dispute: false,
        last_processed_transaction: 0,
        transaction_log: vec![
            TransactionOp::Deposit(1, 100.0000),
            TransactionOp::Deposit(2, 100.0000),
            TransactionOp::Withdraw(3, 50.0000),
            TransactionOp::Dispute(2),
        ],
    };
    process_account(&mut account);
    assert_eq!(account.available_funds, 50.0000);
    assert_eq!(account.held_funds, 100.0000);
    assert_eq!(account.total_funds, 150.0000);
}

#[test]
fn test_process_chargeback() {
    let mut account = Account {
        id: 1,
        available_funds: 0.0,
        held_funds: 0.0,
        total_funds: 0.0,
        locked: false,
        in_dispute: false,
        last_processed_transaction: 0,
        transaction_log: vec![
            TransactionOp::Deposit(1, 100.0000),
            TransactionOp::Deposit(2, 100.0000),
            TransactionOp::Withdraw(3, 50.0000),
            TransactionOp::Dispute(2),
            TransactionOp::Chargeback(2),
        ],
    };
    process_account(&mut account);
    assert_eq!(account.available_funds, 50.0000);
    assert_eq!(account.held_funds, 0.0000);
    assert_eq!(account.total_funds, 50.0000);
    assert_eq!(account.locked, true);
}

#[test]
fn test_process_resolve() {
    let mut account = Account {
        id: 1,
        available_funds: 0.0,
        held_funds: 0.0,
        total_funds: 0.0,
        locked: false,
        in_dispute: false,
        last_processed_transaction: 0,
        transaction_log: vec![
            TransactionOp::Deposit(1, 100.0000),
            TransactionOp::Deposit(2, 100.0000),
            TransactionOp::Withdraw(3, 50.0000),
            TransactionOp::Dispute(2),
            TransactionOp::Resolve(2),
        ],
    };
    process_account(&mut account);
    assert_eq!(account.available_funds, 150.0000);
    assert_eq!(account.held_funds, 0.0000);
    assert_eq!(account.total_funds, 150.0000);
    assert_eq!(account.locked, false);
}

// Process a transaction and update the account balance
fn process_account(account: &mut Account) {
    for transaction in &account.transaction_log {
        // Walk through the transactions in order and process them
        match transaction {
            TransactionOp::Deposit(tx_id, amount) => {
                account.available_funds += amount;
                account.total_funds += amount;
                account.last_processed_transaction = *tx_id;
            }
            TransactionOp::Withdraw(tx_id, amount) => {
                account.available_funds -= amount;
                account.total_funds -= amount;
                account.last_processed_transaction = *tx_id;
            }
            TransactionOp::Dispute(tx_id) => {
                // Find the transaction in the log
                let dispute_op = match find_transaction(*tx_id, &account.transaction_log) {
                    Some(dispute_op) => dispute_op,
                    None => {
                        // if the tx specified doesn't exist, or the tx isn't under dispute, ignore it
                        return;
                    }
                };
                let amount = match find_transaction_amount(dispute_op) {
                    Some(amount) => amount,
                    None => {
                        // if the tx specified doesn't exist, or the tx isn't under dispute, ignore it
                        return;
                    }
                };
                account.in_dispute = true;
                account.available_funds -= amount;
                account.held_funds += amount;
                // Total funds should remain the same
                account.last_processed_transaction = *tx_id;
            }
            TransactionOp::Resolve(tx_id) => {
                let resolve_op = match find_transaction(*tx_id, &account.transaction_log) {
                    Some(resolve_op) => resolve_op,
                    None => {
                        // if the tx specified doesn't exist, or the tx isn't under dispute, ignore it
                        return;
                    }
                };
                let amount = match find_transaction_amount(resolve_op) {
                    Some(amount) => amount,
                    None => {
                        // if the tx specified doesn't exist, or the tx isn't under dispute, ignore it
                        return;
                    }
                };
                account.held_funds -= amount;
                account.available_funds += amount;
            }
            TransactionOp::Chargeback(tx_id) => {
                let chargeback_op = match find_transaction(*tx_id, &account.transaction_log) {
                    Some(chargeback_op) => chargeback_op,
                    None => {
                        // if the tx specified doesn't exist, or the tx isn't under dispute, ignore it
                        return;
                    }
                };
                let amount = match find_transaction_amount(chargeback_op) {
                    Some(amount) => amount,
                    None => {
                        // if the tx specified doesn't exist, or the tx isn't under dispute, ignore it
                        return;
                    }
                };
                account.held_funds -= amount;
                account.total_funds -= amount;
                account.locked = true;
                account.last_processed_transaction = *tx_id;
            }
        }
    }

    account.transaction_log.clear();
}

// Builds up a list of transactions in memory.
fn store_record(record: &RawRecord, accounts: &mut HashMap<u16, Account>) {
    accounts
        .entry(record.client)
        .and_modify(|account| {
            account.transaction_log.push(record.into());
        })
        .or_insert(Account {
            id: record.client,
            available_funds: 0.0,
            held_funds: 0.0,
            total_funds: 0.0,
            locked: false,
            last_processed_transaction: 0,
            in_dispute: false,
            transaction_log: vec![record.into()],
        });
}

fn find_transaction_amount(transaction_op: &TransactionOp) -> Option<f32> {
    match transaction_op {
        TransactionOp::Deposit(_, amount) => Some(*amount),
        TransactionOp::Withdraw(_, amount) => Some(*amount),
        _ => None,
    }
}

fn find_transaction(id: u32, transaction_log: &[TransactionOp]) -> Option<&TransactionOp> {
    transaction_log
        .iter()
        .find(|transaction| match transaction {
            TransactionOp::Deposit(tx_id, _) => *tx_id == id,
            TransactionOp::Withdraw(tx_id, _) => *tx_id == id,
            TransactionOp::Dispute(tx_id) => *tx_id == id,
            TransactionOp::Resolve(tx_id) => *tx_id == id,
            TransactionOp::Chargeback(tx_id) => *tx_id == id,
        })
}

// Insert all checks for the server environment into this function
fn environment_check(csv_file: &Path) -> Result<bool> {
    // Gives a _very_ rough estimate of the line count using worst case scenario of 35 bytes per line
    let csv_file_lines = csv_file.metadata()?.len() / 35;
    let memory_info = sys_info::mem_info()?;
    println!("memory_info avail: {}", memory_info.avail);
    let account_memory_size = mem::size_of::<Account>();

    // Figure out the number of accounts that can safely fit in memory
    // This is a conservative estimate of the number of accounts that can fit in memory
    // because TransactionOps are much smaller than Accounts. This assumes a worst case scenario of
    // no duplicate accounts
    println!("account memory size: {}", account_memory_size);
    let max_accounts = memory_info.avail / account_memory_size as u64;
    println!("max_accounts: {}", max_accounts);

    // Find the number of lines in the file
    if csv_file_lines > max_accounts {
        // This will require batch processing with writing out to disk
        return Ok(false);
    }

    // Everything should fit into memory
    Ok(true)
}

fn print_accounts(accounts: &mut HashMap<u16, Account>) {
    print_output_header();
    for (_, account) in &mut accounts.iter_mut() {
        process_account(account);
        println!("{}", account);
    }
}

fn print_output_header() {
    println!("client, available, held, total, locked");
}

// Takes arguments of one_pass for in memory processing and csv_reader to process
fn run(one_pass: bool, csv_reader: &mut csv::Reader<std::fs::File>) -> Result<()> {
    /*
    if one_pass {
        let mut accounts = HashMap::new();
        let record_iter = csv_reader.deserialize();
        for record in record_iter {
            let record: RawRecord = record?;
            store_record(&record, &mut accounts);
        }
        print_accounts(&mut accounts);
    } else {
        */
    // Too large to process in working memory
    let mut client_accounts: bv::BitArr!(for 65535, in u16) = bv::BitArray::ZERO;
    let sled_db = sled::Config::new().temporary(true).path("sled.db").open()?;
    let record_iter = csv_reader.deserialize();
    // Walk over the records and store them in the sled database
    let mut line_number = 0;
    for record in record_iter {
        let record: RawRecord = record?;
        let client_id = record.client;
        let mut account = match sled_db.get(record.client.to_string())? {
            Some(account) => bincode::deserialize::<Account>(&account)?,
            None => Account {
                id: record.client,
                available_funds: 0.0,
                held_funds: 0.0,
                total_funds: 0.0,
                locked: false,
                last_processed_transaction: 0,
                in_dispute: false,
                transaction_log: Vec::new(),
            },
        };
        // Store each transaction in the account
        line_number += 1;
        println!(
            "Saving line: {} account: {} and transaction: {:?}",
            line_number,
            account,
            TransactionOp::from(&record)
        );
        account.transaction_log.push(record.into());
        sled_db.insert(client_id.to_string(), bincode::serialize(&account)?)?;
        // Save the client ID for later
        client_accounts.set(client_id.into(), true);
    }
    println!("processing finished");
    print_output_header();
    // For each client process the account and print it out
    for client in client_accounts.iter_ones() {
        let mut account: Account = match sled_db.get(client.to_string())? {
            Some(account) => bincode::deserialize::<Account>(&account)?,
            None => {
                // If the account doesn't exist, ignore it
                eprint!("Account {} doesn't exist but should", client);
                continue;
            }
        };
        process_account(&mut account);
        println!("{}", account);
    }
    //}
    Ok(())
}

fn main() -> Result<()> {
    // Take the first argument as the path to the CSV file
    let path = match env::args().nth(1) {
        Some(path) => path,
        None => {
            return Err(anyhow!("Please provide a path to the CSV file"));
        }
    };
    let path = Path::new(&path);
    if !path.exists() {
        return Err(anyhow!("CSV file {} does not exist", path.display()));
    }
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .trim(Trim::All)
        .from_path(path)?;
    let one_pass = environment_check(path)?;

    run(one_pass, &mut csv_reader)?;

    Ok(())
}
