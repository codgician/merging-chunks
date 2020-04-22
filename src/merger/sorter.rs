use rayon::prelude::*;

/*
    Function that implements parallel sort for a vector
    Currently using par_sort_unstable() from rayon crate, may implement one myself in future

    @param v    vector to be sorted
*/
pub fn sort(v: &mut Vec<u64>) {
    v.par_sort_unstable();  
}

/*
    Unit tests
   
    Test sort() against non-parallel sort
*/
#[cfg(test)]
mod tests {
    use super::*;
    use rand::distributions::{Distribution, Uniform};

    #[test]
    fn should_work_for_empty() {
        let mut v: Vec<u64> = Vec::new();
        sort(&mut v);
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn should_work_for_small_cases() {
        const VEC_SIZE: usize = 100;

        let mut rng = rand::thread_rng();
        let dist = Uniform::from(0 .. std::u64::MAX);    

        let mut v: Vec<u64> = vec![0; VEC_SIZE];
        for _ in 0 .. 10 {
            for i in 0 .. VEC_SIZE {
                v[i] = dist.sample(&mut rng);
            }
            let mut expected = v.clone();
            sort(&mut v);
            expected.sort();
            assert_eq!(expected, v);
        }
    }
}
