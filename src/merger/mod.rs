use std::fs::*;
use std::io::*;
use std::option::*;
use std::str::from_utf8;
use std::path::Path;
mod sorter;

/*
    Function exposed to main.rs

    @param chunk_num    number of chunks provided
    @param part_exp     log2(p), where p stands for number of partitions

    @return result Ok / Err

*/
pub fn merge_chunks(chunk_num: usize, part_exp: usize) -> Result<()> {
    if part_exp > 64 {
        return Err(Error::new(ErrorKind::Other, "invalid part_exp"));
    }

    if Path::new("result.txt").exists() {
        remove_file("result.txt")?;
    }

    // Decide which algorithm to use according to chunk_num
    if chunk_num >= (1usize << 30) {
        merge_chunks_large(chunk_num, part_exp)
    } else { 
        merge_chunks_small(chunk_num, part_exp)
    }
}

/*
    Merge chunk for small data sets

    @param chunk_num    number of chunks provided
    @param part_exp     log2(p), where p stands for number of partitions

    @return result Ok / Err

*/
fn merge_chunks_small(chunk_num: usize, part_exp: usize) -> Result<()> {
    /*
        chunk_readers[i]    holds the BufReader instance of file corresponding to ith chunk (i.in)
        chunk_nexts[i]      holds the buffered value (the next one) for ith chunk
        result_writer       BufWriter instance of result file (result.txt)
    */
    let mut chunk_readers: Vec<BufReader<File>> = Vec::new();
    let mut chunk_nexts: Vec<Option<u64>> = Vec::new();
    let mut result_writer = BufWriter::new(File::create("result.txt").expect("create result.txt failed"));
    chunk_readers.reserve_exact(chunk_num);
    chunk_nexts.reserve_exact(chunk_num);
    for i in 0 .. chunk_num {
        let file = File::open(i.to_string() + ".in")?;
        chunk_readers.push(BufReader::new(file));
        chunk_nexts.push(read_next(&mut chunk_readers[i]));
    }

    for i in 0 .. (1u64 << part_exp) {
        // Calculate bounds of ith partition
        let lb = (1 << (64 - part_exp)) * i;
        let ub = if i == 0 { (1 << (64 - part_exp)) - 1 } else { lb - 1 + (1 << (64 - part_exp)) };

        let mut v: Vec<u64> = Vec::new();

        /*
            If chunk_nexts[i]:
                * exceeds upper bound of current partition? skip it!
                * indicates that reader reaches EOF? skip it!
                * otherwise push to vector
        */
        for j in 0 .. chunk_num {
            match chunk_nexts[j] {
                Some(x) => if x <= ub { v.push(x); } else { continue; }, 
                None => continue,
            }

            chunk_nexts[j] = read_chunk(&mut v, &mut chunk_readers[j], ub);
        }        
        sorter::sort(&mut v);
        write_result(&mut result_writer, &v)?;
    }

    result_writer.flush()?;
    Ok(())
}

/*
    Merge chunk for large data sets

    @param chunk_num    number of chunks provided
    @param part_exp     log2(p), where p stands for number of partitions

    @return result Ok / Err

*/
fn merge_chunks_large(chunk_num: usize, part_exp: usize) -> Result<()> {
    /*
        part_writers[i]     holds the BufWriter instance of file corresponding to ith partition (i.out)
        result_writer       BufWriter instance of result file (result.txt)
    */
    let mut part_writers: Vec<BufWriter<std::fs::File>> = Vec::new();
    let mut result_writer = BufWriter::new(File::create("result.txt").expect("create result.txt failed"));
    part_writers.reserve_exact(1 << part_exp);

    // Create .out file for every partition
    for i in 0 .. (1 << part_exp) {
        let file = File::create(i.to_string() + ".out")?;
        part_writers.push(BufWriter::new(file));
    }

    // Read each value of each chunk, put them in corresponding .out file
    for i in 0 .. chunk_num {
        let file = File::open(i.to_string() + ".in")?;
        let mut reader = BufReader::new(file);

        loop {
            let cur = read_next(&mut reader);
            match cur {
                Some(x) => write_to_part(&mut part_writers, x, part_exp)?,
                None => break,
            }
        }
    }

    // Read .out file of each partition, sort then concatenate them
    for i in 0 .. (1 << part_exp) {
        part_writers[i].flush()?;
        let file = File::open(i.to_string() + ".out")?;
        let mut reader = BufReader::new(file);
        let mut v: Vec<u64> = Vec::new();
        read_chunk(&mut v, &mut reader, std::u64::MAX);
        sorter::sort(&mut v);
        write_result(&mut result_writer, &v)?;
    }

    result_writer.flush()?;
    Ok(())
}

/*
    Helper function for merge_chunks_large()
    For u64 value x, determine which partition it belongs to and write it to corresponding .out file

    @param writers      vector of BufWrite instances to .out files
    @param x            value to be determined
    @param part_exp     log2(p), where p stands for number of partitions

    @return result Ok / Err

*/
fn write_to_part(writers: &mut Vec<BufWriter<File>>, x: u64, part_exp: usize) -> Result<()> {
    let part_id: usize = (x / (1 << (64 - part_exp))) as usize;
    write!(&mut writers[part_id], "{} ", x)?;
    Ok(())
}

/*
    Helper function for merge_chunks_small()
    Read a chunk, take all values no bigger than a specified upper_bound and write them in a vector

    @param v            vector that holds the taken values
    @param reader       BufReader instance for file
    @param ub           specified upper_bound

    @return the first value that is larger than the upper_bound. None if it does not exist

*/
fn read_chunk(v: &mut Vec<u64>, reader: &mut BufReader<File>, ub: u64) -> Option<u64> {
    loop {
        let cur = read_next(reader);
        match cur {
            Some(x) => if x <= ub { v.push(x); } else { return Some(x); },
            None => return None,
        }
    }
}

/*
    Helper function 
    Read the next u64 value from file

    @param reader       BufReader instance for file

    @return the next u64 value. None if reaches end of file

*/
fn read_next(reader: &mut BufReader<File>) -> Option<u64> {
    let mut cur: Vec<u8> = Vec::new();

    loop {
        reader.read_until(b' ', &mut cur).expect("read chunk failed");

        match cur.last() {
            Some(&x) => if x == b' ' { cur.pop(); } else { },
            None => return None,
        }

        if cur.is_empty() {
            continue;
        }

        return Some(from_utf8(&cur).unwrap().parse().expect("not an u64"))
    }
}

/*
    Helper function 
    Take a vector of values and write them into a file

    @param writer       BufWriter instance for file

    @return the next u64 value. None if reaches end of file

*/
fn write_result(writer: &mut BufWriter<File> , v: &Vec<u64>) -> Result<()> {
    for i in v {
        write!(writer, "{} ", i)?;
    }
    Ok(())
}


/*
    Unit tests

    Generate 10 small random data sets and test both two merging functions
    Data sets will be no larger than 100 * 100
*/
#[cfg(test)]
mod tests {
    use super::*;

    use rand::distributions::{Distribution, Uniform};
    use serial_test::serial;

    fn generate_chunk(len: usize, lb: u64, ub: u64) -> Vec<u64> {
        if lb >= ub {
            panic!("incorrect bounds");
        }

        let mut rng = rand::thread_rng();
        let dist = Uniform::from(lb .. ub);    

        let mut v = Vec::new(); 
        v.reserve_exact(len);
        for _ in 0 .. len {
            v.push(dist.sample(&mut rng));
        }

        v.sort();
        v
    }

    fn write_chunk(id: usize, v: &Vec<u64>) -> Result<()> {
        let file = File::create(id.to_string() + ".in")?;
        let mut writer = BufWriter::new(file);

        for i in v {
            write!(writer, "{} ", i)?;
        }

        writer.flush()?;
        Ok(())
    }

    fn cleanup_test(chunk_num: usize, part_num: usize) -> Result<()> {
        for i in 0 .. chunk_num {
            remove_file(i.to_string() + ".in")?;
        }
        for i in 0 .. part_num {
            remove_file(i.to_string() + ".out")?;
        }
        remove_file("result.txt")?;
        Ok(())
    }

    fn validate_result(expected: &Vec<u64>) -> bool {
        let file = File::open("result.txt").expect("open result.txt failed");
        let mut reader = BufReader::new(file);

        let mut result = String::new();
        reader.read_to_string(&mut result).expect("read result.txt failed");

        let v: Vec<u64> = result.split_whitespace()
                            .map(|x| x.parse::<u64>().expect("not an u64"))
                            .collect();

        if v.len() != expected.len() {
            return false;
        }

        for i in 0 .. v.len() {
            if v[i] != expected[i] {
                return false;
            }
        }

        true
    }

    fn do_test(is_large: bool, chunk_num: usize, expected: &Vec<u64>) {
        if is_large {
            merge_chunks_large(chunk_num, 9).expect("merging failed");
        } else {
            merge_chunks_small(chunk_num, 9).expect("merging failed");
        }
        assert_eq!(validate_result(&expected), true);
        if is_large {
            cleanup_test(chunk_num, 1 << 9).expect("cleanup failed");
        } else {
            cleanup_test(chunk_num, 0).expect("cleanup failed");
        }
    } 
    
    #[test]
    #[serial]
    fn merge_chunk_small_should_work() {
        for _ in 0 .. 10 {
            const LIM: usize = 100;

            let mut expected: Vec<u64> = Vec::new();
            expected.reserve(LIM * LIM);

            let mut rng = rand::thread_rng();
            let dist = Uniform::from(1 .. LIM);    
            for i in 0 .. LIM {
                let cur_chunk = generate_chunk(dist.sample(&mut rng), 0, std::u64::MAX);
                write_chunk(i, &cur_chunk).expect("write chunk failed");
                expected.extend(&cur_chunk);
            }

            expected.sort();
            do_test(false, LIM, &expected);
        }
    }

    #[test]
    #[serial]
    fn merge_chunk_large_should_work() {
        for _ in 0 .. 10 {
            const LIM: usize = 100;

            let mut expected: Vec<u64> = Vec::new();
            expected.reserve(LIM * LIM);

            let mut rng = rand::thread_rng();
            let dist = Uniform::from(1 .. LIM);    
            for i in 0 .. LIM {
                let cur_chunk = generate_chunk(dist.sample(&mut rng), 0, std::u64::MAX);
                write_chunk(i, &cur_chunk).expect("write chunk failed");
                expected.extend(&cur_chunk);
            }

            expected.sort();
            do_test(true, LIM, &expected);
        }
    }
}
