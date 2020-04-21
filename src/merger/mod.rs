use std::fs::*;
use std::io::*;
use std::option::*;
use std::str::from_utf8;
use std::path::Path;

mod sorter;

pub fn merge_chunks(chunk_num: usize, part_exp: usize) -> Result<()> {
    if part_exp > 64 {
        return Err(Error::new(ErrorKind::Other, "invalid part_exp"));
    }

    if Path::new("result.txt").exists() {
        remove_file("result.txt")?;
    }

    let mut chunk_readers: Vec<BufReader<std::fs::File>> = Vec::new();
    let mut chunk_nexts: Vec<Option<u64>> = Vec::new();
    chunk_readers.reserve_exact(chunk_num);
    chunk_nexts.reserve_exact(chunk_num);
    for i in 0 .. chunk_num {
        chunk_readers.push(BufReader::new(File::open(i.to_string() + ".in").unwrap()));
        chunk_nexts.push(read_next(&mut chunk_readers[i]));
    }


    for i in 0 .. (1u64 << part_exp) {
        let lb = (1u64 << (64 - part_exp)) * (i as u64);
        let ub = if i == 0 { (1u64 << (64 - part_exp)) - 1 } else { lb - 1 + (1u64 << (64 - part_exp)) };

        let mut v: Vec<u64> = Vec::new();

        for j in 0 .. chunk_num {
            match chunk_nexts[j] {
                Some(x) => if x <= ub { v.push(x); } else { continue; }, 
                None => continue,
            }

            chunk_nexts[j] = read_chunk(&mut v, &mut chunk_readers[j], ub);
        }        
        sorter::sort(&mut v);
        write_result(&v)?;
    }

    Ok(())
}

fn read_next(reader: &mut BufReader<std::fs::File>) -> Option<u64> {
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

fn read_chunk(v: &mut Vec<u64>, reader: &mut BufReader<std::fs::File>, ub: u64) -> Option<u64> {
    loop {
        let cur = read_next(reader);
        match cur {
            Some(x) => if x <= ub { v.push(x); } else { return Some(x); },
            None => return None,
        }
    }
}

fn write_result(v: &Vec<u64>) -> Result<()> {
    let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open("result.txt")
                    .unwrap();

    let mut writer = BufWriter::new(file);
    for i in v {
        write!(writer, "{} ", i)?;
    }

    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate rand;
    use rand::distributions::{Distribution, Uniform};

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

    fn cleanup_test(chunk_num: usize) -> Result<()> {
        for i in 0 .. chunk_num {
            remove_file(i.to_string() + ".in")?;
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

    fn do_test(chunk_num: usize, expected: &Vec<u64>) {
        merge_chunks(chunk_num, 9).expect("merging failed");
        assert_eq!(validate_result(&expected), true);
        cleanup_test(chunk_num).expect("cleanup failed");
    } 
    
    #[test]
    fn should_work_for_small_random_cases() {
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
            do_test(LIM, &expected);
        }
    }
}
