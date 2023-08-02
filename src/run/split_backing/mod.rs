use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
};

use self::backing_wrapper::BackingWrapper;

mod backing_wrapper;

pub struct SplitView<T> {
    backing: Arc<Mutex<backing_wrapper::BackingWrapper<T>>>,
    // a past-the-end index of the current segment in the file
    segment_end: u64,
    // the current read position inside this segment
    current_index: u64,
}

impl<T> SplitView<T>
where
    T: Seek + Read + Send,
{
    pub fn new(mut backing: T) -> io::Result<Self> {
        let segment_end = backing.seek(SeekFrom::End(0))?;
        Ok(Self {
            backing: Arc::new(Mutex::new(BackingWrapper::new(backing))),
            segment_end,
            current_index: 0,
        })
    }
    pub fn add_segment(&mut self) -> io::Result<SplitViewWrite<T>> {
        let segment_start = self.backing.lock().unwrap().seek(SeekFrom::End(0))?;
        Ok(SplitViewWrite {
            backing: self.backing.clone(),
            length: 0,
            segment_start,
        })
    }
}

pub struct SplitViewWrite<T> {
    backing: Arc<Mutex<BackingWrapper<T>>>,
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

impl<T> Write for SplitViewWrite<T>
where
    T: Write + Seek,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let bytes_written = self
            .backing
            .lock()
            .unwrap()
            .write_at(self.segment_start + self.length, buf)?;
        self.length += bytes_written as u64;
        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.backing.lock().unwrap().flush()
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
        let bytes_read = self.backing.lock().unwrap().read(self.current_index, buf)?;
        self.current_index += bytes_read as u64;
        Ok(bytes_read)
    }
}

#[cfg(test)]
mod test {
    use std::io::{Cursor, Write};

    use super::SplitView;

    #[test]
    fn test_flush() {
        let file = Cursor::new(vec![]);
        let mut wrapper = SplitView::new(file).unwrap();
        let mut view = wrapper.add_segment().unwrap();
        view.flush().unwrap();
    }
}
