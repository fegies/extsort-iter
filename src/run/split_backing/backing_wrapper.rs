use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

/// A wrapper around a seekable file.
pub(crate) struct BackingWrapper<T> {
    /// the wrapped file
    inner: T,
    /// Determines if we can rely on the cached seek position.
    tainted: bool,
    /// the currently active seek index.
    current_seek_index: u64,
}

impl<T: Seek> BackingWrapper<T> {
    fn ensure_offset(&mut self, start_offset: u64) -> io::Result<()> {
        if self.tainted || self.current_seek_index != start_offset {
            self.inner.seek(SeekFrom::Start(start_offset))?;
            self.current_seek_index = start_offset;
            self.tainted = false;
        }
        Ok(())
    }
}

impl<T> BackingWrapper<T>
where
    T: Seek + Read,
{
    /// reads from the specfied position in the file
    pub(crate) fn read(&mut self, start_offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        self.ensure_offset(start_offset)?;

        let res = self.inner.read(buffer);
        match &res {
            Ok(bytes_read) => self.current_seek_index += *bytes_read as u64,
            Err(_) => {
                self.tainted = true;
            }
        }
        res
    }

    pub(crate) fn seek(&mut self, seek: SeekFrom) -> io::Result<u64> {
        let pos = self.inner.seek(seek)?;
        self.current_seek_index = pos;
        self.tainted = false;
        Ok(pos)
    }

    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner,
            current_seek_index: 0,
            tainted: true,
        }
    }
}

impl<T> BackingWrapper<T>
where
    T: Seek + Write,
{
    pub(crate) fn write_at(&mut self, start_offset: u64, buf: &[u8]) -> io::Result<usize> {
        self.ensure_offset(start_offset)?;
        self.inner.write_all(buf)?;
        Ok(buf.len())
    }
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
