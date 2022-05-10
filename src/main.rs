use core::fmt;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::env;
use std::fmt::Formatter;

use csv_async::Trim;
use std::process::exit;
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::RwLock;

use serde_derive::Deserialize;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[macro_use]
extern crate error_chain;

error_chain! {

    errors{
         AmountNotPositive{}
         LockedBalance{}
         FundsInsufficientForGivenOperation{}
         InvalidArgument{}
         UnknownTransationType{}
         DecimalFormatError{}
         TransactionAlreadyExist{}
         TransactionAlreadyInDispute{}
         ReferenceTransactionTypeIncorrect{}
         ReferenceTransactionNotFound{}
         ReferenceTransactionIncorrect{}
         ReferenceTransactionStateIncorrect{}
    }
    foreign_links{
        Io(::std::io::Error);
        Decimal(::rust_decimal::Error);
        CSV(csv_async::Error);
    }
}

type CommandType = String;
type ClientIdType = u16;
type TransactionIdType = u32;

#[derive(Debug, Deserialize)]
struct Command {
    #[serde(rename = "type")]
    type_: String,
    #[serde(rename = "client")]
    client_id: ClientIdType,
    #[serde(rename = "tx")]
    tx_id: TransactionIdType,
    #[serde(rename = "amount")]
    amount: Option<String>,
}

type AmountType = Option<Decimal>;
const ZERO_AMOUNT: Decimal = Decimal::ZERO;

struct Transaction {
    type_: CommandType,
    client_id: ClientIdType,
    amount: AmountType,
    pub in_dispute: bool,
}
type TransactionHistoryType = Arc<RwLock<HashMap<TransactionIdType, Transaction>>>;
struct TransactionHistory;
impl TransactionHistory {
    pub fn new() -> TransactionHistoryType {
        Arc::new(RwLock::new(HashMap::new()))
    }
}

trait BalanceOperation
where
    Self: Sized,
{
    fn deposit(&self, amount: Decimal) -> Result<Self>;
    fn withdrawal(&self, amount: Decimal) -> Result<Self>;
    fn dispute(&self, amount: Decimal) -> Result<Self>;
    fn resolve(&self, amount: Decimal) -> Result<Self>;
    fn chargeback(&self, amount: Decimal) -> Result<Self>;
}

const DEPOSIT: &str = "deposit";
const WITHDRAWAL: &str = "withdrawal";
const DISPUTE: &str = "dispute";
const RESOLVE: &str = "resolve";
const CHARGEBACK: &str = "chargeback";

#[derive(Copy, Clone)]
struct Balance {
    avail: Decimal,
    held: Decimal,
    locked: bool,
}

impl Balance {
    pub fn new() -> Self {
        Self {
            avail: ZERO_AMOUNT,
            held: ZERO_AMOUNT,
            locked: false,
        }
    }
}
impl fmt::Display for Balance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.avail,
            self.held,
            self.avail + self.held,
            self.locked
        )
    }
}

fn bail_if_locked(balance: &Balance) -> Result<()> {
    if balance.locked {
        bail!(ErrorKind::LockedBalance)
    } else {
        Ok(())
    }
}

fn check_amount(amount: Decimal) -> Result<()> {
    if amount <= ZERO_AMOUNT {
        Err(ErrorKind::AmountNotPositive.into())
    } else {
        Ok(())
    }
}

fn to_decimal(n: &str) -> Result<Decimal> {
    let d = Decimal::from_str_radix(n, 10)?;
    if d.scale() > 4 {
        Err(ErrorKind::DecimalFormatError.into())
    } else {
        Ok(d)
    }
}

impl BalanceOperation for Balance {
    fn deposit(&self, amount: Decimal) -> Result<Self> {
        bail_if_locked(self)?;

        Ok(Balance {
            avail: self.avail + amount,
            ..*self
        })
    }

    fn withdrawal(&self, amount: Decimal) -> Result<Self> {
        bail_if_locked(self)?;

        if self.avail < amount {
            Err(ErrorKind::FundsInsufficientForGivenOperation.into())
        } else {
            Ok(Balance {
                avail: self.avail - amount,
                ..*self
            })
        }
    }

    fn dispute(&self, amount: Decimal) -> Result<Self> {
        bail_if_locked(self)?;

        if self.avail < amount {
            Err(ErrorKind::FundsInsufficientForGivenOperation.into())
        } else {
            Ok(Balance {
                avail: self.avail - amount,
                held: self.held + amount,
                ..*self
            })
        }
    }

    fn resolve(&self, amount: Decimal) -> Result<Self> {
        bail_if_locked(self)?;

        if self.held < amount {
            Err(ErrorKind::FundsInsufficientForGivenOperation.into())
        } else {
            Ok(Balance {
                avail: self.avail + amount,
                held: self.held - amount,
                ..*self
            })
        }
    }

    fn chargeback(&self, amount: Decimal) -> Result<Self> {
        bail_if_locked(self)?;

        if self.held < amount {
            Err(ErrorKind::FundsInsufficientForGivenOperation.into())
        } else {
            Ok(Balance {
                avail: self.avail,
                held: self.held - amount,
                locked: true,
            })
        }
    }
}

type BalancesType = Arc<RwLock<HashMap<ClientIdType, Balance>>>;
struct Balances;
impl Balances {
    fn new() -> BalancesType {
        Arc::new(RwLock::new(HashMap::new()))
    }
}

async fn do_cmd(
    cmd: &Command,
    transaction_history: &TransactionHistoryType,
    balances: &BalancesType,
) -> Result<()> {
    // check the transaction logic first
    {
        let guard = transaction_history.read().await;

        match cmd.type_.as_str() {
            DEPOSIT | WITHDRAWAL => {
                if guard.contains_key(&cmd.tx_id) {
                    bail!(ErrorKind::TransactionAlreadyExist)
                }
            }
            DISPUTE => {
                if let Some(tx) = guard.get(&cmd.tx_id) {
                    if tx.type_.as_str() != DEPOSIT {
                        bail!(ErrorKind::ReferenceTransactionTypeIncorrect);
                    }
                    if tx.client_id != cmd.client_id {
                        bail!(ErrorKind::ReferenceTransactionIncorrect);
                    }
                    if tx.in_dispute {
                        bail!(ErrorKind::TransactionAlreadyInDispute);
                    }
                } else {
                    bail!(ErrorKind::ReferenceTransactionNotFound)
                }
            }

            RESOLVE | CHARGEBACK => {
                if let Some(tx) = guard.get(&cmd.tx_id) {
                    if !tx.in_dispute {
                        bail!(ErrorKind::ReferenceTransactionStateIncorrect);
                    }
                } else {
                    return Err(ErrorKind::ReferenceTransactionNotFound.into());
                }
            }
            _ => return Err(ErrorKind::UnknownTransationType.into()),
        }
    }
    // check if amount is available for an operation
    if let Some(amount) = match cmd.type_.as_str() {
        DISPUTE | RESOLVE | CHARGEBACK => transaction_history
            .read()
            .await
            .get(&cmd.tx_id)
            .and_then(|tx| tx.amount),
        DEPOSIT | WITHDRAWAL => match &cmd.amount {
            Some(q) => Some(to_decimal(q.as_str())?),
            None => None,
        },
        _ => unreachable!(),
    } {
        // execute balance change
        check_amount(amount)?;
        let client_id = cmd.client_id;
        let mut p = balances.write().await;
        let balance = p.entry(client_id).or_insert_with(Balance::new);
        let new_balance = match cmd.type_.as_str() {
            DEPOSIT => balance.deposit(amount)?,
            WITHDRAWAL => balance.withdrawal(amount)?,
            DISPUTE => balance.dispute(amount)?,
            RESOLVE => balance.resolve(amount)?,
            CHARGEBACK => balance.chargeback(amount)?,
            _ => unreachable!(),
        };
        p.insert(client_id, new_balance);

        {
            // insert into or update the history
            let mut guard = transaction_history.write().await;
            match cmd.type_.as_str() {
                DISPUTE => {
                    guard.entry(cmd.tx_id).and_modify(|tx| tx.in_dispute = true);
                }
                RESOLVE | CHARGEBACK => {
                    guard
                        .entry(cmd.tx_id)
                        .and_modify(|tx| tx.in_dispute = false);
                }
                DEPOSIT | WITHDRAWAL => {
                    guard.insert(
                        cmd.tx_id,
                        Transaction {
                            type_: cmd.type_.clone(),
                            client_id: cmd.client_id,
                            amount: Some(amount),
                            in_dispute: false,
                        },
                    );
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    } else {
        Err(ErrorKind::UnknownTransationType.into())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    use tokio_stream::StreamExt;

    let args: Vec<String> = env::args().collect();
    if args.len() > 2 {
        println!("Usage: {} filename", args[0]);
        exit(-1);
    }
    let (ingress, mut egress) = mpsc::unbounded_channel();
    let h: JoinHandle<Result<()>> = tokio::spawn(async move {
        let reader = File::open(&args[1]).await?;

        let mut csv_rdr = csv_async::AsyncReaderBuilder::new()
            .flexible(true)
            .trim(Trim::All)
            .create_deserializer(reader);

        let mut records = csv_rdr.deserialize::<Command>();
        while let Some(input) = records.next().await {
            match input {
                Ok(cmd) => {
                    ingress.send(cmd).unwrap();
                }
                Err(e) => println!("{}", e),
            }
        }
        Ok(())
    });

    let balances = Balances::new();
    let transaction_history = TransactionHistory::new();

    let g = tokio::spawn(async move {
        loop {
            match egress.recv().await {
                Some(cmd) => {
                    if let Err(e) = do_cmd(&cmd, &transaction_history, &balances).await {
                        eprintln!("\"{:?}\" : {}", cmd, e);
                    }
                }
                None => break,
            }
        }
        println!("client,available,held, total, locked");
        for balance in balances.read().await.iter() {
            println!("{},{}", balance.0, balance.1);
        }
    });

    h.await.unwrap()?;
    g.await.unwrap();

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use crate::{
        do_cmd, Balances, BalancesType, ClientIdType, Result, TransactionHistory,
        TransactionHistoryType,
    };
    use crate::{Command, ErrorKind};
    use csv_async::Trim;
    use rust_decimal::Decimal;
    use tokio_stream::StreamExt;

    async fn consume(th: &TransactionHistoryType, bs: &BalancesType, data: &str) -> Result<()> {
        let mut rdr = csv_async::AsyncReaderBuilder::new()
            .flexible(true)
            .trim(Trim::All)
            .create_deserializer(data.as_bytes());

        let mut records = rdr.deserialize::<Command>();
        while let Some(input) = records.next().await {
            match input {
                Ok(cmd) => {
                    if let Err(e) = do_cmd(&cmd, th, bs).await {
                        return Err(e);
                    }
                }
                Err(e) => {
                    eprintln!("{}", e);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn basic_deserialization_test() -> Result<()> {
        let balances = Balances::new();
        let txh = TransactionHistory::new();

        consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        deposit, 1, 1, 1000
        withdrawal, 1, 2, 500
        deposit, 1, 3, 500
        dispute, 1, 3
        resolve, 1, 3",
        )
        .await?;

        Ok(())
    }
    #[tokio::test]
    async fn basic_account_test() -> Result<()> {
        let balances = Balances::new();
        let txh = TransactionHistory::new();

        consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        deposit, 1, 1, 1000
        deposit, 1, 2, 500
        withdrawal, 1, 3, 1500
        deposit, 2, 4, 1000
        withdrawal, 2, 5 , 1 ",
        )
        .await?;

        let b = balances.read().await;
        let b1 = b.get(&(1 as ClientIdType));
        assert!(b1.is_some());
        let b1 = b1.unwrap();
        assert_eq!(b1.avail, Decimal::ZERO);
        assert_eq!(b1.held, Decimal::ZERO);
        assert!(!b1.locked);

        let b2 = b.get(&(2 as ClientIdType));
        assert!(b2.is_some());
        let b2 = b2.unwrap();
        assert_eq!(b2.avail, Decimal::new(999, 0));
        assert_eq!(b1.held, Decimal::ZERO);
        assert!(!b1.locked);

        assert!((3..5).map(|x| b.get(&x)).all(|x| x.is_none()));

        assert_eq!(txh.read().await.iter().count(), 5);

        let e = consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        withdrawal, 1, 30, -1000",
        )
        .await
        .unwrap_err();

        assert!(match e.0 {
            ErrorKind::AmountNotPositive => true,
            _ => false,
        });

        let e = consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        withdrawal, 1, 31, 0",
        )
        .await
        .unwrap_err();
        assert!(match e.0 {
            ErrorKind::AmountNotPositive => true,
            _ => false,
        });

        Ok(())
    }

    #[tokio::test]
    async fn check_decimal_precision() -> Result<()> {
        let balances = Balances::new();
        let txh = TransactionHistory::new();

        consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        deposit, 1, 1, 1000.0001
        deposit, 1, 2, 499.9999",
        )
        .await?;

        let b = balances.read().await;
        let b1 = b.get(&(1 as ClientIdType));
        assert!(b1.is_some());
        let b1 = b1.unwrap();
        assert_eq!(b1.avail, Decimal::new(15000000, 4));

        assert!(consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        deposit, 1, 3, 1000.00000
        deposit, 1, 4, 499.99999"
        )
        .await
        .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn transaction_consistency() -> Result<()> {
        let balances = Balances::new();
        let txh = TransactionHistory::new();

        consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        deposit, 1, 1, 1000
        deposit, 1, 2, 500
        withdrawal, 1, 3, 500
        deposit, 2, 4, 1000
        withdrawal, 2, 5 , 1 ",
        )
        .await?;

        let e = consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        deposit, 1, 1, 1000",
        )
        .await
        .unwrap_err();
        assert!(match e.0 {
            ErrorKind::TransactionAlreadyExist => true,
            _ => false,
        });

        let e = consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        withdrawal, 1, 1, ",
        )
        .await
        .unwrap_err();
        assert!(match e.0 {
            ErrorKind::TransactionAlreadyExist => true,
            _ => false,
        });

        assert!(consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        dispute, 1, 2",
        )
        .await
        .is_ok());

        assert!(consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        resolve, 1, 2"
        )
        .await
        .is_ok());

        let e = consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        dispute, 1, 10",
        )
        .await
        .unwrap_err();
        assert!(match e.0 {
            ErrorKind::ReferenceTransactionNotFound => true,
            _ => false,
        });

        let e = consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        resolve, 1, 10",
        )
        .await
        .unwrap_err();

        assert!(match e.0 {
            ErrorKind::ReferenceTransactionNotFound => true,
            _ => false,
        });

        assert!(consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        dispute, 1, 2",
        )
        .await
        .is_ok());

        assert!(consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        chargeback, 1, 2"
        )
        .await
        .is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn balance_locking_test() -> Result<()> {
        let balances = Balances::new();
        let txh = TransactionHistory::new();

        let e = consume(
            &txh,
            &balances,
            "\
        type ,  client, tx, amount
        deposit, 1, 1, 1000
        dispute, 1, 1
        chargeback, 1, 1
        withdrawal, 1, 2 , 1",
        )
        .await
        .unwrap_err();

        assert!(match e.0 {
            ErrorKind::LockedBalance => true,
            _ => false,
        });

        Ok(())
    }
}
