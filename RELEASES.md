## 0.3.1
This is a release only improving crate internals with no changes to the public interface
### Changed:
- Changed the buffer flush strategy to write on a background thread while the main thread
    can resume filling the buffer
- Close (and delete) sort files as soon as they are exhausted, not just on next tree rebuild

## 0.3.0
### New:
- The returned Iterator is now `Send` where possible
- Added an optional compression feature to compress the runs written to disk and reduce IO

### Changed:
- Simplified the Configuration creation by removing the associated type for improved ergonomics
### Deprecreated:
- `ExtsortConfig::default_for` and `ExtsortConfig::create_with_buffer_size_for`.
as there are now methods to create the config without needing the associated type

## 0.2.1
Fixed an issue where the crate would not build on stable rust due to usage of unstable NonZeroUsize::MIN constant

## 0.2.0
Reworked crate internals.
### New:
- Replaced Heap-Based merge algorithm with Loser tree implementation
- Limit maximum number of sort files to 256 by sharing files between runs when required

## 0.1.0
Initial working version
