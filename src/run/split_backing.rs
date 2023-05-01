use std::{
    cell::RefCell,
    io::{self, Read, Seek, SeekFrom, Write},
    rc::Rc,
};

/// A wrapper around a seekable file.
struct BackingWrapper<T> {
    /// the wrapped file
    inner: T,
    /// Determins if we can rely on the cached seek position.
    tainted: bool,
    /// the currently active seek index.
    current_seek_index: u64,
}

impl<T> BackingWrapper<T>
where
    T: Seek + Read,
{
    /// reads from the specfied position in the file
    fn read(&mut self, start_offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        if self.tainted || self.current_seek_index != start_offset {
            self.inner.seek(SeekFrom::Start(start_offset))?;
            self.current_seek_index = start_offset;
            self.tainted = false;
        }

        let res = self.inner.read(buffer);
        match &res {
            Ok(bytes_read) => self.current_seek_index += *bytes_read as u64,
            Err(_) => {
                self.tainted = true;
            }
        }
        res
    }

    fn new(inner: T) -> Self {
        Self {
            inner,
            current_seek_index: 0,
            tainted: true,
        }
    }
}

pub struct SplitView<T> {
    backing: Rc<RefCell<BackingWrapper<T>>>,
    // a past-the-end index of the current segment in the file
    segment_end: u64,
    // the current read position inside this segment
    current_index: u64,
}
impl<T> SplitView<T>
where
    T: Seek + Read,
{
    pub fn new(mut backing: T) -> io::Result<Self> {
        let segment_end = backing.seek(SeekFrom::End(0))?;
        Ok(Self {
            backing: Rc::new(RefCell::new(BackingWrapper::new(backing))),
            segment_end,
            current_index: 0,
        })
    }
    pub fn add_segment(&mut self) -> io::Result<SplitViewWrite<T>> {
        let segment_start = self.backing.borrow_mut().inner.seek(SeekFrom::End(0))? + 1;
        Ok(SplitViewWrite {
            backing: self.backing.clone(),
            length: 0,
            segment_start,
        })
    }
}

pub struct SplitViewWrite<T> {
    backing: Rc<RefCell<BackingWrapper<T>>>,
    segment_start: u64,
    length: u64,
}
impl<T> From<SplitViewWrite<T>> for SplitView<T> {
    fn from(val: SplitViewWrite<T>) -> Self {
        let segment_end = val.segment_start + val.length;
        SplitView {
            backing: val.backing,
            segment_end,
            current_index: val.segment_start,
        }
    }
}

impl<T: Write> Write for SplitViewWrite<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let bytes_written = self.backing.borrow_mut().inner.write(buf)?;
        self.length += bytes_written as u64;
        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.backing.borrow_mut().inner.flush()
    }
}

impl<T> Read for SplitView<T>
where
    T: Seek + Read,
{
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let remaining_len = (self.segment_end - self.current_index) as usize;
        if remaining_len < buf.len() {
            buf = &mut buf[..remaining_len];
        }
        if buf.is_empty() {
            return Ok(0);
        }
        let bytes_read = self.backing.borrow_mut().read(self.current_index, buf)?;
        self.current_index += bytes_read as u64;
        Ok(bytes_read)
    }
}
