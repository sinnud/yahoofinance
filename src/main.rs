use yahoo_finance::history;

fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

fn main() {
   // retrieve 6 months worth of data for Apple
   let data = history::retrieve("AAPL").unwrap();

   // print the date and closing price for each day we have data
   for bar in &data {
      println!("On {} Apple closed at ${:.2}", bar.timestamp.format("%b %e %Y"), bar.close)
   }

    println!("length of data: {}", data.len());
    let bar = &data[data.len()-1];
    println!("Type of last of data");
    print_type_of(&bar);
}
