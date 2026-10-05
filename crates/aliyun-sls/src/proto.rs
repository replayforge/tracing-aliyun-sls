use compact_str::CompactString;
use std::hash::Hash;
use std::sync::Arc;
use std::{borrow::Borrow, io, io::Write};

cfg_if::cfg_if! {
    if #[cfg(all(feature = "inline-keypairs-16", not(feature = "inline-none")))] {
        pub(crate) const N_INLINE_KEY_PAIR: usize = 16;
    } else if #[cfg(all(feature = "inline-keypairs-8", not(feature = "inline-none")))] {
        pub(crate) const N_INLINE_KEY_PAIR: usize = 8;
    } else if #[cfg(all(feature = "inline-keypairs-4", not(feature = "inline-none")))] {
        pub(crate) const N_INLINE_KEY_PAIR: usize = 4;
    } else if #[cfg(all(feature = "inline-keypairs-2", not(feature = "inline-none")))] {
        pub(crate) const N_INLINE_KEY_PAIR: usize = 2;
    } else {
        pub(crate) const N_INLINE_KEY_PAIR: usize = 0;
    }
}

cfg_if::cfg_if! {
    if #[cfg(all(feature = "inline-tags-16", not(feature = "inline-none")))] {
        pub(crate) const N_INLINE_TAGS: usize = 16;
    } else if #[cfg(all(feature = "inline-tags-8", not(feature = "inline-none")))] {
        pub(crate) const N_INLINE_TAGS: usize = 8;
    } else if #[cfg(all(feature = "inline-tags-4", not(feature = "inline-none")))] {
        pub(crate) const N_INLINE_TAGS: usize = 4;
    } else if #[cfg(all(feature = "inline-tags-2", not(feature = "inline-none")))] {
        pub(crate) const N_INLINE_TAGS: usize = 2;
    } else {
        pub(crate) const N_INLINE_TAGS: usize = 0;
    }
}

type Map<K, V, const N: usize> = litemap::LiteMap<K, V, smallvec::SmallVec<(K, V), N>>;

/// MayStaticKey is a key that can be either a static string or a shared string.
#[derive(Debug, Clone, Ord, Eq)]
pub enum MayStaticKey {
    /// Static string, which is a `&'static str`.
    Static(&'static str),
    /// Shared string, which is an `Arc<CompactString>`.
    Shared(Arc<CompactString>),
}

/// Log entry with a timestamp and fixed capacity key-value pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log {
    /// UNIX Time Format
    timestamp: u32,
    /// for time nano part
    subsec_nanosecond: Option<u32>,
    /// log contents key value pairs
    contents: Map<MayStaticKey, CompactString, N_INLINE_KEY_PAIR>,
}

/// Metadata for a group of logs, including topic, source, and fixed capacity key-value tags.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogGroupMetadata {
    topic: CompactString,
    source: CompactString,
    log_tags: Map<MayStaticKey, CompactString, N_INLINE_TAGS>,
}

impl MayStaticKey {
    /// Create a new `MayStaticKey` from a string.
    pub fn new(s: impl Into<CompactString>) -> Self {
        MayStaticKey::Shared(Arc::new(s.into()))
    }

    /// Create a new `MayStaticKey` from a static string slice.
    pub const fn from_static(s: &'static str) -> Self {
        MayStaticKey::Static(s)
    }
}

impl AsRef<str> for MayStaticKey {
    fn as_ref(&self) -> &str {
        match self {
            MayStaticKey::Static(s) => s,
            MayStaticKey::Shared(s) => s.as_ref(),
        }
    }
}

impl<S: AsRef<str>> PartialEq<S> for MayStaticKey {
    fn eq(&self, other: &S) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl<S: AsRef<str>> PartialOrd<S> for MayStaticKey {
    fn partial_cmp(&self, other: &S) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl Borrow<str> for MayStaticKey {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

impl Hash for MayStaticKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl Default for Log {
    fn default() -> Self {
        Log::now()
    }
}

impl Default for LogGroupMetadata {
    fn default() -> Self {
        LogGroupMetadata::new()
    }
}

impl Log {
    /// Create a new log with the current timestamp.
    pub fn now() -> Self {
        let now = jiff::Timestamp::now();
        Log {
            timestamp: now.as_second() as u32,
            subsec_nanosecond: Some(now.subsec_nanosecond() as u32),
            contents: Map::new(),
        }
    }

    /// Create a new log with the specified timestamp and optional subsecond nanosecond.
    pub fn new(timestamp: u32, subsec_nanosecond: Option<u32>) -> Self {
        Log {
            timestamp,
            subsec_nanosecond,
            contents: Map::new(),
        }
    }

    /// Modify the timestamp of the log.
    pub fn with_timestamp(mut self, timestamp: u32) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Modify the subsecond nanosecond of the log.
    pub fn with_subsec_nanosecond(mut self, subsec_nanosecond: u32) -> Self {
        self.subsec_nanosecond = Some(subsec_nanosecond);
        self
    }

    /// Modify the timestamp of the log.
    pub fn modify_timestamp(&mut self, timestamp: u32) -> &mut Self {
        self.timestamp = timestamp;
        self
    }

    /// Modify the subsecond nanosecond of the log.
    pub fn modify_subsec_nanosecond(&mut self, subsec_nanosecond: u32) -> &mut Self {
        self.subsec_nanosecond = Some(subsec_nanosecond);
        self
    }

    /// Add a key-value pair to the log contents.
    #[inline]
    pub fn with(mut self, key: MayStaticKey, value: impl Into<CompactString>) -> Self {
        self.contents.insert(key, value.into());
        self
    }

    /// Add a key-value pair to the log contents.
    pub fn insert(&mut self, key: MayStaticKey, value: impl Into<CompactString>) -> &mut Self {
        self.contents.insert(key, value.into());
        self
    }

    /// Remove a key-value pair from the log contents.
    pub fn remove<Q>(&mut self, key: &str) -> &mut Self {
        self.contents.remove(key);
        self
    }
}

impl LogGroupMetadata {
    /// Create a new log group metadata with default values.
    pub fn new() -> Self {
        LogGroupMetadata {
            topic: CompactString::const_new(""),
            source: CompactString::const_new(""),
            log_tags: Map::new(),
        }
    }

    /// Set the topic for the log group metadata.
    pub fn with_topic(mut self, topic: impl Into<CompactString>) -> Self {
        self.topic = topic.into();
        self
    }

    /// Set the source for the log group metadata.
    pub fn with_source(mut self, source: impl Into<CompactString>) -> Self {
        self.source = source.into();
        self
    }

    /// Add a tag to the log group metadata.
    pub fn with_tag(mut self, key: MayStaticKey, value: impl Into<CompactString>) -> Self {
        self.log_tags.insert(key, value.into());
        self
    }

    /// Add a tag to the log group metadata.
    pub fn add_tag(&mut self, key: MayStaticKey, value: impl Into<CompactString>) -> &mut Self {
        self.log_tags.insert(key, value.into());
        self
    }

    /// Remove a tag from the log group metadata.
    pub fn remove_tag<Q>(&mut self, key: &str) -> &mut Self {
        self.log_tags.remove(key);
        self
    }
}

// Manual implementation for faster encoding
pub(crate) fn encode_log_group<W: Write>(
    writer: &mut W,
    metadata: &LogGroupMetadata,
    logs: &[Log],
) -> io::Result<()> {
    for log in logs {
        encode_message(1u32, log, writer)?;
    }
    if !metadata.topic.is_empty() {
        encode_str(3u32, &metadata.topic, writer)?;
    }
    if !metadata.source.is_empty() {
        encode_str(4u32, &metadata.source, writer)?;
    }
    for tag in metadata.log_tags.iter() {
        encode_message(6u32, &tag, writer)?;
    }

    Ok(())
}

pub(crate) fn calc_log_group_encoded_len(metadata: &LogGroupMetadata, logs: &[Log]) -> usize {
    calc_log_group_metadata_encoded_len(metadata)
        + logs
            .iter()
            .map(calc_log_group_log_encoded_len)
            .sum::<usize>()
}

/// Calculate the encoded size contributed by metadata in a log group.
pub(crate) fn calc_log_group_metadata_encoded_len(metadata: &LogGroupMetadata) -> usize {
    (!metadata.topic.is_empty())
        .then(|| encoded_str_len(3u32, &metadata.topic))
        .unwrap_or(0)
        + (!metadata.source.is_empty())
            .then(|| encoded_str_len(4u32, &metadata.source))
            .unwrap_or(0)
        + encoded_len_repeated(6u32, metadata.log_tags.iter(), metadata.log_tags.len())
}

/// Calculate the incremental encoded size of one log in a log group.
#[inline]
pub(crate) fn calc_log_group_log_encoded_len(log: &Log) -> usize {
    let len = log.encoded_len();
    key_len(1u32) + encoded_len_varint(len as u64) + len
}

#[cfg(feature = "persist")]
pub(crate) fn encode_persisted_event(
    metadata: &LogGroupMetadata,
    log: &Log,
) -> io::Result<Vec<u8>> {
    let mut bytes =
        Vec::with_capacity(calc_log_group_metadata_encoded_len(metadata) + log.encoded_len() + 32);
    bytes.extend_from_slice(b"SLS1");
    write_persisted_str(&mut bytes, &metadata.topic)?;
    write_persisted_str(&mut bytes, &metadata.source)?;
    write_persisted_pairs(&mut bytes, metadata.log_tags.iter())?;
    bytes.extend_from_slice(&log.timestamp.to_le_bytes());
    match log.subsec_nanosecond {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.push(0),
    }
    write_persisted_pairs(&mut bytes, log.contents.iter())?;
    Ok(bytes)
}

#[cfg(feature = "persist")]
pub(crate) fn decode_persisted_event(bytes: &[u8]) -> io::Result<(LogGroupMetadata, Log)> {
    let mut input = PersistedInput::new(bytes);
    if input.take(4)? != b"SLS1" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported persisted event version",
        ));
    }
    let topic = input.string()?;
    let source = input.string()?;
    let mut metadata = LogGroupMetadata::new()
        .with_topic(topic)
        .with_source(source);
    for (key, value) in input.pairs()? {
        metadata.add_tag(MayStaticKey::new(key), value);
    }
    let timestamp = input.u32()?;
    let subsec_nanosecond = match input.byte()? {
        0 => None,
        1 => Some(input.u32()?),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid persisted nanosecond marker",
            ));
        }
    };
    let mut log = Log::new(timestamp, subsec_nanosecond);
    for (key, value) in input.pairs()? {
        log.insert(MayStaticKey::new(key), value);
    }
    if !input.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing persisted event bytes",
        ));
    }
    Ok((metadata, log))
}

#[cfg(feature = "persist")]
fn write_persisted_pairs<K, V>(
    output: &mut Vec<u8>,
    pairs: impl Iterator<Item = (K, V)>,
) -> io::Result<()>
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    let pairs = pairs.collect::<Vec<_>>();
    let count = u32::try_from(pairs.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many persisted pairs"))?;
    output.extend_from_slice(&count.to_le_bytes());
    for (key, value) in pairs {
        write_persisted_str(output, key.as_ref())?;
        write_persisted_str(output, value.as_ref())?;
    }
    Ok(())
}

#[cfg(feature = "persist")]
fn write_persisted_str(output: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "persisted string too long"))?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

#[cfg(feature = "persist")]
struct PersistedInput<'a> {
    bytes: &'a [u8],
    offset: usize,
}

#[cfg(feature = "persist")]
impl<'a> PersistedInput<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "persisted event length overflow",
            )
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "truncated persisted event")
        })?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> io::Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid u32"))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn string(&mut self) -> io::Result<String> {
        let len = usize::try_from(self.u32()?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid string length"))?;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    fn pairs(&mut self) -> io::Result<Vec<(String, String)>> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid pair count"))?;
        let mut pairs = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            pairs.push((self.string()?, self.string()?));
        }
        Ok(pairs)
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

trait Message {
    fn encode_into_vec<W: Write>(&self, writer: &mut W) -> io::Result<()>;
    fn encoded_len(&self) -> usize;
}

impl<T: Message> Message for &T {
    #[inline]
    fn encode_into_vec<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        T::encode_into_vec(self, writer)
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        T::encoded_len(self)
    }
}

impl<K: AsRef<str>, V: AsRef<str>> Message for (K, V) {
    #[inline]
    fn encode_into_vec<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        encode_str(1u32, self.0.as_ref(), writer)?;
        encode_str(2u32, self.1.as_ref(), writer)
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        encoded_str_len(1u32, self.0.as_ref()) + encoded_str_len(2u32, self.1.as_ref())
    }
}

impl Message for Log {
    #[inline]
    fn encode_into_vec<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        encode_varint_field(1u32, self.timestamp as u64, writer)?;
        for msg in &self.contents {
            encode_message(2u32, &msg, writer)?;
        }
        if let Some(value) = self.subsec_nanosecond {
            encode_fixed32(4u32, value, writer)?;
        }
        Ok(())
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        encoded_varint_field_len(1u32, self.timestamp as u64)
            + encoded_len_repeated(2u32, self.contents.iter(), self.contents.len())
            + self
                .subsec_nanosecond
                .as_ref()
                .map_or(0, |_| encoded_fixed32_len(4u32))
    }
}

// Copy from prost

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum WireType {
    Varint = 0,
    LengthDelimited = 2,
    ThirtyTwoBit = 5,
}

#[inline]
fn encode_varint<W: Write>(mut value: u64, writer: &mut W) -> io::Result<()> {
    loop {
        if value < 0x80 {
            writer.write_all(&[value as u8])?;
            break;
        } else {
            writer.write_all(&[((value & 0x7F) | 0x80) as u8])?;
            value >>= 7;
        }
    }
    Ok(())
}

#[inline]
fn encode_key<W: Write>(tag: u32, wire_type: WireType, writer: &mut W) -> io::Result<()> {
    let key = (tag << 3) | wire_type as u32;
    encode_varint(u64::from(key), writer)
}

#[inline]
fn encode_varint_field<W: Write>(tag: u32, value: u64, writer: &mut W) -> io::Result<()> {
    encode_key(tag, WireType::Varint, writer)?;
    encode_varint(value, writer)
}

#[inline]
fn encode_fixed32<W: Write>(tag: u32, value: u32, writer: &mut W) -> io::Result<()> {
    encode_key(tag, WireType::ThirtyTwoBit, writer)?;
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

#[inline]
fn encode_message<W: Write>(tag: u32, msg: &impl Message, writer: &mut W) -> io::Result<()> {
    encode_key(tag, WireType::LengthDelimited, writer)?;
    encode_varint(msg.encoded_len() as u64, writer)?;
    msg.encode_into_vec(writer)
}

#[inline]
fn encode_str<W: Write>(tag: u32, value: impl AsRef<str>, writer: &mut W) -> io::Result<()> {
    let value = value.as_ref();
    encode_key(tag, WireType::LengthDelimited, writer)?;
    encode_varint(value.len() as u64, writer)?;
    writer.write_all(value.as_bytes())?;
    Ok(())
}

#[inline]
fn encoded_len_varint(value: u64) -> usize {
    // Based on [VarintSize64][1].
    // [1]: https://github.com/google/protobuf/blob/3.3.x/src/google/protobuf/io/coded_stream.h#L1301-L1309
    ((((value | 1).leading_zeros() ^ 63) * 9 + 73) / 64) as usize
}

#[inline]
fn key_len(tag: u32) -> usize {
    encoded_len_varint(u64::from(tag << 3))
}

#[inline]
fn encoded_str_len(tag: u32, value: impl AsRef<str>) -> usize {
    let value = value.as_ref();
    key_len(tag) + encoded_len_varint(value.len() as u64) + value.len()
}

#[inline]
fn encoded_len_repeated<I, M>(tag: u32, messages: I, len: usize) -> usize
where
    I: Iterator<Item = M>,
    M: Message,
{
    key_len(tag) * len
        + messages
            .map(|m| m.encoded_len())
            .map(|len| len + encoded_len_varint(len as u64))
            .sum::<usize>()
}

#[inline]
fn encoded_varint_field_len(tag: u32, value: u64) -> usize {
    key_len(tag) + encoded_len_varint(value)
}

#[inline]
fn encoded_fixed32_len(tag: u32) -> usize {
    key_len(tag) + 4
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size() {
        println!("size_of::<Log>() = {}", size_of::<Log>());
        println!(
            "size_of::<LogGroupMetadata>() = {}",
            size_of::<LogGroupMetadata>()
        );
    }

    #[test]
    fn encoded_len_matches_actual_output_with_topic_and_source() {
        let metadata = LogGroupMetadata::new()
            .with_topic("topic")
            .with_source("source")
            .with_tag(MayStaticKey::from_static("tag"), "value");
        let logs = [
            Log::new(1, None).with(MayStaticKey::from_static("a"), "b"),
            Log::new(2, Some(3)).with(MayStaticKey::from_static("long"), "value"),
        ];
        let mut encoded = Vec::new();

        encode_log_group(&mut encoded, &metadata, &logs).unwrap();

        assert_eq!(calc_log_group_encoded_len(&metadata, &logs), encoded.len());
        assert_eq!(
            calc_log_group_metadata_encoded_len(&metadata)
                + logs
                    .iter()
                    .map(calc_log_group_log_encoded_len)
                    .sum::<usize>(),
            encoded.len()
        );
    }

    #[test]
    fn empty_topic_and_source_do_not_add_encoded_bytes() {
        let metadata = LogGroupMetadata::new();
        let logs = [Log::new(1, None)];
        let mut encoded = Vec::new();

        encode_log_group(&mut encoded, &metadata, &logs).unwrap();

        assert_eq!(calc_log_group_encoded_len(&metadata, &logs), encoded.len());
        assert_eq!(calc_log_group_metadata_encoded_len(&metadata), 0);
    }
}
