use std::env;
mod merger;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() <= 1 {
        println!("Usage: merging-chunks <chunk_num>");
        return;
    }

    let chunk_num: usize = args[1].parse::<usize>().expect("invalid chunk_num");
    merger::merge_chunks(chunk_num, 9).expect("merging failed"); // partition into 2^9 = 512 parts
}
