
extern crate rayon;

use rayon::prelude::*;

pub fn sort(vec: &mut Vec<u64>) {
    vec.par_sort_unstable();  
}

#[cfg(test)]
mod tests {
    use super::*;
    
    extern crate rand;
    use rand::distributions::{Distribution, Uniform};

    #[test]
    fn sort_should_work_for_small_case() {
        let mut v: Vec<u64> = [3, 2, 5, 6, 4, 1].to_vec();
        sort(&mut v);
        assert_eq!(v, [1, 2, 3, 4, 5, 6].to_vec());
    }

    #[test]
    fn sort_should_work_for_empty() {
        let mut v: Vec<u64> = Vec::new();
        sort(&mut v);
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn sort_should_work_for_large_case() {
        let mut rng = rand::thread_rng();
        let dist = Uniform::from(0 .. std::u64::MAX);

        let mut v: Vec<u64> = Vec::new();
        for _ in 0 .. 30000 {
            v.push(dist.sample(&mut rng));
        }
        sort(&mut v);

        for i in 1 .. 30000 {
            assert_eq!(v[i] >= v[i - 1], true);
        }
    }
}

