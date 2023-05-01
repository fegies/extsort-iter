use std::io::Read;

use crate::{merge::LoserTree, run::file_run::ExternalRun};

pub type ResultIterator<T, O> = LoserTree<T, ExternalRun<T, Box<dyn Read>>, O>;
