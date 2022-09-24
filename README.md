# extsort-iter [![license](https://img.shields.io/github/license/fegies/extsort-iter.svg)](https://github.com/fegies/extsort-iter/blob/master/LICENSE) ![GitHub Workflow Status](https://img.shields.io/github/workflow/status/fegies/extsort-iter/Rust)

### Sort iterators of any type as long as they fit on your disk!

`extsort-iter` is a library that allows you to externally
sort any iterator with no serialization steps required.

Conceptually, we are taking your input iterator, buffering it, sorting the runs,
moving the data to disk before streaming it back in order.

## Why this crate?

Because we offer a very nice high level API and perform our sort without any
(de)serialization overhead.

## Usage

```rust
let data = [1,42,3,41,5];

// lets pretend this is an iterator that will not fit in memory completely
let source = data.into_iter(); 
let config = ExtsortConfig::default_for::<usize>();
let iterator = data.external_sort(config);

// do whatever you want with the sorted iterator
for item in iterator { 
    println!("{}", item);
}

// alternatively you can provide a comparison function
// (here one that sorts 42 to the beginning because it is the best number)
let iterator = data.external_sort_by(config, |a,b| if a == 42 {
    Ordering::Less
}else {
    a.cmp(b)
});

// or you can provide a key extraction function to sort by
// (here we are sorting by the number of trailing ones)
let iterator = data.external_sort_by_key(config, |a| a.trailing_ones());
```

If you enable the `parallel_sort` feature, parallel versions of all sort function
will become available that sort the run buffer in parallel using rayon:

```rust
// normal sort by the ord impl
let iterator = data.par_external_sort(config);

// sort using a comparison function
let iterator = data.par_external_sort_by(config, |a,b| if a == 42 {
    Ordering::Less
}else {
    a.cmp(b)
});

// parallel sort using key extraction function
let iterator = data.par_external_sort_by_key(config, |a| a.trailing_ones());
```

## When not to use this crate

When your source iterator is big because each item owns large amounts of heap memory.
That means the following case will result in memory exhaustion:
```rust
let data = "somestring".to_owned();
let iterator = std::iter::from_fn(|| Some(data.clone())).take(1_000_000);
let sorted  = iterator.external_sort(ExtsortConfig::default_for::<String>());
```

The reason for that is that we are not dropping the values from the source iterator until they are 
yielded by the result iterator.

You can think of it as buffering the entire input iterator, with the values
themselves living on disk but all memory the values point to still living on the heap.

## Unsafe Usage

This crate uses unsafe code to view a run buffer as a byteslice to copy it to disk
and to read data from disk back to memory and treat is as our values again.

All unsafe code is limited to the [file_run](https://github.com/fegies/extsort-iter/blob/master/src/run/file_run.rs) module, is fairly well documented and tested and the testsuite passes
when run with [miri](https://github.com/rust-lang/miri), so we are as sure as we can reasonably be about the code being correct and sound.