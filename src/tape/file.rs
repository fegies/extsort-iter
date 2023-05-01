use std::fs;
use std::fs::File;
use std::{io, path::Path};

#[cfg(windows)]
pub fn create_file(filename: &Path) -> io::Result<File> {
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .custom_flags(winapi::FILE_FLAG_DELETE_ON_CLOSE)
        .open(filename)
}

#[cfg(not(windows))]
/// Creates the file that we want to use for the run later.
pub fn create_file(filename: &Path) -> io::Result<File> {
    let file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(filename)?;

    // we immediately delete the file, but keep the handle open
    // this has 2 advantages:
    // - we make it much harder to modify the file outside our program
    //   because it is no longer accessible from the file system (just /proc)
    // - it will automatically be cleaned up for us when the handle is dropped (or when the program exits)
    //   eliminating the need for custom cleanup code in our program.
    fs::remove_file(filename)?;

    Ok(file)
}
