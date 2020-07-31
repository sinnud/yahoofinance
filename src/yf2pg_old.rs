// collect data from yahoo finance
// store data into postgres database
use yahoo_finance::{history, Interval};
// use postgres::{Client, NoTls};
use chrono::{DateTime, Utc};
//data structure for yahoo finance history
pub struct YfBar{
    pub yf_stockname: String,
    // copy from https://docs.rs/crate/yahoo-finance/0.2.0/source/src/lib.rs
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64
}

// extern crate chrono;
// use chrono::{Duration, Local};
pub fn yf_hist(stockname: &str) -> Vec<YfBar> {
    let stcnm = stockname;
    // retrieve 6 months worth of data for Apple
    // let data = history::retrieve("AAPL").unwrap();
    // let data = history::retrieve(stcnm).unwrap();
    // let data = history::retrieve_interval("AAPL", Interval::_6mo).unwrap();
    let data = history::retrieve_interval(stcnm, Interval::_6mo).unwrap();
    //let allowdt = Local::now() + Duration::days(-10);
    let mut rst: Vec<YfBar> = Vec::new();
    for bar in &data {
	//if bar.timestamp<allowdt {continue;}
	//println!("On {} Apple closed at ${:.2}", bar.timestamp.format("%b %e %Y"), bar.close);
	rst.push(YfBar{
	    yf_stockname: String::from(stcnm),
	    timestamp: bar.timestamp,
	    open: bar.open,
	    high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume
	});
   }
    rst
}

extern crate postgres;
use postgres::{Client, NoTls};

// macros to convert data type into string slice for cat
macro_rules! pg_num2str{
    ($intvar:expr, $strvar:ident) => {
	let t=$intvar.to_string();
	let $strvar : &str = &t;
    }
}
macro_rules! pg_ts2str{
    ($intvar:expr, $strvar:ident) => {
	let t=$intvar.format("%Y-%m-%d %H:%M:%S").to_string();
	let $strvar : &str = &t;
    }
}


pub fn hist2pg(hist : &mut Vec<YfBar>) {
    println!("Length of hist: {}", hist.len());
    
    // host: localhost
    // username: rust
    // password: rust
    // database: rust
    let constr = "host=localhost user=rust password=rust";
    let mut client = Client::connect(constr, NoTls).unwrap();
    //let pgdb="rust";
    //client.batch_execute("select count(*) from wdinfo.music243").unwrap();
    
    // check schema existence. If not exist, exit program
    let pg_skm="yahoof";
    let query = ["select count(*) from pg_catalog.pg_namespace where nspname = '"
		 , pg_skm
		 , "'"
                ].concat();
    let thisqry: &str = &query[..]; // convert string to &str
    //println!("Query: {}", thisqry);
    let pgrst=client.query(thisqry, &[]).unwrap();
    for row in pgrst{
	let rst: i64=row.get(0);
	//println!("Query result: {}", rst);
	if rst != 1{
	    println!("Error query {} result: {}!!!", thisqry, rst);
	    std::process::exit(1);
	}
    }

    // check table existence. If not exist, create one
    let pg_tbl="yf_hist";
    let query = ["SELECT count(*) FROM information_schema.tables WHERE table_schema = '"
		 , pg_skm
		 , "' AND table_name = '"
		 , pg_tbl
		 , "'"
                ].concat();
    let thisqry: &str = &query[..]; // convert string to &str
    //println!("Query: {}", thisqry);
    let pgrst=client.query(thisqry, &[]).unwrap();
    for row in pgrst{
	let rst: i64=row.get(0);
	//println!("Query result: {}", rst);
	if rst != 1{
	    println!("Table {} does NOT exist, create it now!!!", pg_tbl);
	    let query = ["set search_path="
			 , pg_skm
			 , "; create table "
			 , pg_tbl
			 , " (yf_stockname text, "
			 , "stock_dt timestamp, "
			 , "open numeric(14, 2), "
			 , "high numeric(14, 2), "
			 , "low numeric(14, 2), "
			 , "close numeric(14, 2), "
			 , "volumn bigint)"
	                ].concat();
	    let thisqry: &str = &query[..]; // convert string to &str
	    println!("Batch queries: {}", thisqry);
	    client.batch_execute(thisqry).unwrap();
	    println!("Table {} created!", pg_tbl);
	}
    }

    // import data into postgres table
    for row in hist{
	// println!("{} on {} closed at ${:.2}"
	// 	 , row.yf_stockname
	// 	 , row.timestamp.format("%Y-%m-%d %H:%M:%S")
	// 	 , row.close
	//         );
	let stockname : &str = &row.yf_stockname; // string to slice
	pg_ts2str!(row.timestamp,   dtstr);
	pg_num2str!(row.open  ,   openstr);
	pg_num2str!(row.high  ,   highstr);
	pg_num2str!(row.low   ,    lowstr);
	pg_num2str!(row.close ,  closestr);
	pg_num2str!(row.volume, volumestr);
	let query = ["set search_path="
		     , pg_skm
		     , "; insert into "
		     , pg_tbl
		     , " values('"
		     , stockname
		     , "', '"
		     , dtstr
		     , "', "
		     , openstr
		     , ", "
		     , highstr
		     , ", "
		     , lowstr
		     , ", "
		     , closestr
		     , ", "
		     , volumestr
		     , ")"
	            ].concat();
	let thisqry: &str = &query; // convert string to string slice
	//println!("Batch queries: {}", thisqry);
	client.batch_execute(thisqry).unwrap();
	//println!("Record inserted to {}!", pg_tbl);
    }
}
