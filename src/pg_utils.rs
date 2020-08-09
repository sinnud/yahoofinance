// postgres utilities
extern crate postgres;
use postgres::{Client, NoTls};
use std::io::Write;

pub struct PgUtils{
    host: String,
    user: String,
    password: String,
}

impl Default for PgUtils {
    fn default() -> Self {
	PgUtils {
	    host: "localhost".to_string(),
	    user: "rust".to_string(),
	    password: "rust".to_string(),
	}
    }
}

impl PgUtils {
    pub fn pg_conn(self: &Self) -> Client {
	let constr = ["host=", &(self.host)
		      , " user=", &(self.user)
		      , " password=", &(self.password)
	             ].concat();
	Self::pg_conn_conf(self, constr)
    }
    pub fn pg_conn_conf(self: &Self, constr: String) -> Client {
	let thisstr: &str = &constr; // convert string to string slice
	Client::connect(thisstr, NoTls).unwrap()
    }
    pub fn pg_skm_exist(self: &Self, conn: &mut Client, schema: String) -> bool {
	let query = ["select count(*) from pg_catalog.pg_namespace where nspname = '"
		 , &schema
		 , "'"
                ].concat();
	let thisqry: &str = &query[..]; // convert string to &str
	let pgrst=conn.query(thisqry, &[]).unwrap();
	for row in pgrst{
	    let rownum: i64=row.get(0);
	    if  rownum == 0{return false;}
	    break;
	}
	return true;
    }
    pub fn pg_tbl_exist(self: &Self, conn: &mut Client
			, schema: String, table: String
                       ) -> bool {
	let query = ["SELECT count(*) FROM information_schema.tables WHERE table_schema = '"
		 , &schema
		 , "' AND table_name = '"
		 , &table
		 , "'"
                ].concat();
	let thisqry: &str = &query[..]; // convert string to &str
	let pgrst=conn.query(thisqry, &[]).unwrap();
	for row in pgrst{
	    let rownum: i64=row.get(0);
	    if  rownum == 0{return false;}
	    break;
	}
	return true;
    }
    pub fn pg_create_tbl(self: &Self, conn: &mut Client
			 , schema: String, table: String
			 , tbl_str: String
    ){
	let query = ["create table "
		 , &schema
		 , "."
		 , &table
		 , " ("
		 , &tbl_str
		 , ")"
                ].concat();
	println!("Debug: query: {}", query);
	let thisqry: &str = &query[..]; // convert string to &str
	conn.batch_execute(thisqry).unwrap();
    }
    pub fn pg_truncate_tbl(self: &Self, conn: &mut Client
			 , schema: String, table: String
    ){
	let query = ["truncate table "
		 , &schema
		 , "."
		 , &table
                ].concat();
	let thisqry: &str = &query[..]; // convert string to &str
	conn.batch_execute(thisqry).unwrap();
    }
    pub fn pg_import_data2tbl(self: &Self, conn: &mut Client
			 , schema: String, table: String
			 , data_str: String
    ){
	let query = ["copy "
		 , &schema
		 , "."
		 , &table
		 , " from stdin"
                ].concat();
	let thisqry: &str = &query[..]; // convert string to &str
	let mut writer = conn.copy_in(thisqry).unwrap();
	writer.write_all(data_str.as_bytes()).unwrap();
	writer.finish().unwrap();
    }
}
