use std::io::{Read, Write};

#[derive(Clone, Copy, Default)]
pub enum CompressionCodec {
    #[default]
    NoCompression,
    #[cfg(feature = "compression_lz4_flex")]
    Lz4Flex,
}

impl CompressionCodec {
    pub fn write_all(self, writer: &mut impl Write, data: &[u8]) -> std::io::Result<()> {
        match self {
            CompressionCodec::NoCompression => writer.write_all(data),
            #[cfg(feature = "compression_lz4_flex")]
            CompressionCodec::Lz4Flex => {
                use ::lz4_flex::frame::FrameEncoder;
                let mut encoder = FrameEncoder::new(writer);
                encoder.write_all(data)?;
                encoder.finish()?;
                Ok(())
            }
        }
    }
    pub fn get_reader(self, inner: impl Read + 'static) -> Box<dyn Read> {
        match self {
            CompressionCodec::NoCompression => Box::new(inner),
            #[cfg(feature = "compression_lz4_flex")]
            CompressionCodec::Lz4Flex => {
                let reader = lz4_flex::frame::FrameDecoder::new(inner);
                Box::new(reader)
            }
        }
    }
}
