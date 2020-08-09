// collect data from yahoo finance
// store data into postgres database
use yahoo_finance::{history, Interval};
// extern crate chrono;
use chrono::Utc;
// use chrono::{Duration, Local};
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
pub fn yf_hist(stockname: &str) -> Vec<String> {
    let stcnm = stockname;
    let data = history::retrieve_interval(stcnm, Interval::_6mo).unwrap();
    //let allowdt = Local::now() + Duration::days(-5);
    let mut rst = Vec::new();
    //for (i, bar) in (&data).iter().enumerate() {
    for bar in &data{
	//if bar.timestamp<allowdt {continue;}
	let mut elm: String = String::new();
	elm.push_str(stcnm);elm.push('\t');
	pg_ts2str!(bar.timestamp,   dtstr);
	pg_num2str!(bar.open  ,   openstr);
	pg_num2str!(bar.high  ,   highstr);
	pg_num2str!(bar.low   ,    lowstr);
	pg_num2str!(bar.close ,  closestr);
	pg_num2str!(bar.volume, volumestr);
	elm.push_str(    dtstr);elm.push('\t');
	elm.push_str(  openstr);elm.push('\t');
	elm.push_str(  highstr);elm.push('\t');
	elm.push_str(   lowstr);elm.push('\t');
	elm.push_str( closestr);elm.push('\t');
	elm.push_str(volumestr);
	rst.push(elm);
    }
    rst
}

extern crate postgres;
//use postgres::{Client, NoTls};
//use std::io::Write;
//use crate::pg_utils::pg_conn_conf;
//use crate::pg_utils::pg_conn;
use crate::pg_utils::PgUtils;
pub fn hist2pg(hist : &mut Vec<String>) {
    let pu=PgUtils::default();
    let mut client = pu.pg_conn();
    let pg_skm="yahoof";
    if !pu.pg_skm_exist(&mut client, pg_skm.to_string()){
	println!("Schema {} does NOT exist!!!", pg_skm);
	std::process::exit(1);
    }
    let pg_tbl="yf_hist";
    if !pu.pg_tbl_exist(&mut client
			, pg_skm.to_string()
			, pg_tbl.to_string()
                       ){
	println!("Table {}.{} does NOT exist!!!", pg_skm, pg_tbl);
	let tbl_str="yf_stockname text, stock_dt timestamp, open numeric(14, 2), high numeric(14, 2), low numeric(14, 2), close numeric(14, 2), volumn bigint, inserteddatetime timestamp".to_string();
	pu.pg_create_tbl(&mut client, pg_skm.to_string(), pg_tbl.to_string()
			 , tbl_str);
    }
    pu.pg_truncate_tbl(&mut client
			, pg_skm.to_string()
			, pg_tbl.to_string()
    );
    //let data=hist.join("\n");
    let mut rst = Vec::new();
    for bar in hist{
	let mut elm: String = String::new();
	let dt = Utc::now();pg_ts2str!(dt, dt_str);
	elm.push_str(bar);elm.push('\t');elm.push_str(dt_str);
	rst.push(elm);
    }
    let data=rst.join("\n");
    pu.pg_import_data2tbl(&mut client
		       , pg_skm.to_string()
		       , pg_tbl.to_string()
		       , data
    );
}
 
