// import yahoo finance data into postgres database
mod yf2pg;
use yf2pg::{yf_hist, hist2pg};
fn main() {
    let mut data = yf_hist();
    hist2pg(&mut data);
}
