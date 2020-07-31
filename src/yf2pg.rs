// collect data from yahoo finance
// store data into postgres database
use yahoo_finance::{history, Interval};
// extern crate chrono;
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
use postgres::{Client, NoTls};
use std::io::Write;
pub fn hist2pg(hist : &mut Vec<String>) {
    // println!("Length of hist: {}", hist.len());
    let constr = "host=localhost user=rust password=rust";
    let mut client = Client::connect(constr, NoTls).unwrap();
    let pg_skm="yahoof";
    let pg_tbl="yf_hist";
    let query =["COPY ",pg_skm,".",pg_tbl," FROM stdin"].concat();
    let thisqry: &str = &query; // convert string to string slice
    let mut writer = client.copy_in(thisqry).unwrap();
    let data=hist.join("\n");
    writer.write_all(data.as_bytes()).unwrap();
    writer.finish().unwrap();
}
 
