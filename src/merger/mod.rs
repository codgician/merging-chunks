use std::fs::*;
use std::io::*;
use std::path::Path;

mod sorter;

pub fn merge_chunks(chunk_num: usize, part_exp: usize) -> Result<()> {
    if part_exp > 64 {
        return Err(Error::new(ErrorKind::Other, "invalid part_exp"));
    }

    if Path::new("result.txt").exists() {
        remove_file("result.txt")?;
    }

    for i in 0 .. (1u64 << part_exp) {
        let lb = (1u64 << (64 - part_exp)) * (i as u64);
        let ub = if lb == 0 { (1u64 << (64 - part_exp)) - 1 } else { lb - 1 + (1u64 << (64 - part_exp)) };

        let mut vec: Vec<u64> = Vec::new();
        read_chunks(&mut vec, chunk_num, lb, ub)?;
        sorter::sort(&mut vec);
        write_result(& vec)?;
    }

    Ok(())
}

fn read_chunks(vec: &mut Vec<u64>, chunk_num: usize, lb: u64, ub: u64) -> Result<()> {
    vec.clear();

    for i in 0 .. chunk_num {
        let file = File::open(i.to_string() + ".in")?;
        let mut reader = BufReader::new(file);

        let mut inputs = String::new();
        reader.read_to_string(&mut inputs)?;

        let numbers: Vec<u64> = inputs.split_whitespace()
                            .map(|x| x.parse::<u64>().expect("not an u64"))
                            .filter(|&x| x >= lb && x <= ub)
                            .collect();
        vec.extend(&numbers);
    }

    Ok(())
}

fn write_result(vec: & Vec<u64>) -> Result<()> {
    let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open("result.txt")
                    .unwrap();

    let mut writer = BufWriter::new(file);
    for i in vec {
        write!(writer, "{} ", i)?;
    }

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

        let mut vec = Vec::new(); 
        vec.reserve_exact(len);
        for _ in 0 .. len {
            vec.push(dist.sample(&mut rng));
        }

        vec.sort();
        vec
    }

    fn write_chunk(id: usize, vec: & Vec<u64>) -> Result<()> {
        let file = File::create(id.to_string() + ".in")?;
        let mut writer = BufWriter::new(file);

        for i in vec {
            write!(writer, "{} ", i)?;
        }

        Ok(())
    }

    fn generate_data(num: usize, size: usize) -> Result<()> {
        for i in 0 .. num {
            write_chunk(i, & generate_chunk(size, 0, std::u64::MAX))?;
        }
    
        Ok(())
    }

    fn cleanup_data(chunk_num: usize) -> Result<()> {
        for i in 0 .. chunk_num {
            remove_file(i.to_string() + ".in")?;
        }

        Ok(())
    }

    fn validate_result() -> bool {
        let file = File::open("result.txt").expect("open result.txt failed");
        let mut reader = BufReader::new(file);

        let mut result = String::new();
        reader.read_to_string(&mut result).expect("read result.txt failed");

        let v: Vec<u64> = result.split_whitespace()
                            .map(|x| x.parse::<u64>().expect("not an u64"))
                            .collect();

        for i in 1 .. v.len() {
            if v[i - 1] > v[i] {
                println!("last = {}, next = {}", v[i - 1], v[i]);
                return false;
            }
        }

        true
    }

    fn do_test(chunk_num: usize, chunk_size: usize) {
        generate_data(chunk_num, chunk_size).expect("data generation failed");
        merge_chunks(chunk_num, 9).expect("merging failed");
        cleanup_data(chunk_num).expect("cleanup failed");
        assert_eq!(validate_result(), true);
    } 

    #[test]
    fn merger_should_work_for_small_cases() {
        do_test(100, 100);
    }
}
