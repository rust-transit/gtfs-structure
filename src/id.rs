use crate::WithId;
use core::marker::PhantomData;
use std::{collections::HashMap, iter::FromIterator};

/// Typed Id over a [Collection]
#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Id<T> {
    id: String,
    #[serde(skip)]
    #[derivative(Debug(bound = ""))]
    #[derivative(Debug = "ignore")]
    #[derivative(Clone(bound = ""))]
    #[derivative(Eq(bound = ""))]
    #[derivative(PartialEq(bound = ""))]
    #[derivative(Hash(bound = ""))]
    _phantom: PhantomData<T>,
}

impl<T> Id<T> {
    fn must_exists(s: String) -> Id<T> {
        Id {
            id: s,
            _phantom: PhantomData,
        }
    }

    /// get as str
    pub fn as_str(&self) -> &str {
        self
    }
}

impl<T> std::ops::Deref for Id<T> {
    type Target = str;
    fn deref(&self) -> &str {
        &self.id
    }
}

/// Collection with typed Ids
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection<T>(HashMap<Id<T>, T>);

impl<T> Default for Collection<T> {
    fn default() -> Self {
        Collection(HashMap::default())
    }
}

impl<T> Collection<T> {
    pub fn get_id(&self, raw_id: &str) -> Option<Id<T>> {
        let id = Id::must_exists(raw_id.to_owned());
        if self.0.contains_key(&id) {
            Some(id)
        } else {
            None
        }
    }
    pub(crate) fn get_by_str(&self, raw_id: &str) -> Option<(Id<T>, &T)> {
        let id = Id::must_exists(raw_id.to_owned());
        self.0.get(&id).map(|v| (id, v))
    }

    pub(crate) fn get_mut_by_str(&mut self, raw_id: &str) -> Option<(Id<T>, &mut T)> {
        let id = Id::must_exists(raw_id.to_owned());
        self.0.get_mut(&id).map(|v| (id, v))
    }

    pub fn get(&self, id: &Id<T>) -> Option<&T> {
        self.0.get(id)
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T: WithId> FromIterator<T> for Collection<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut c = Self::default();

        for i in iter {
            let _ = c.0.insert(Id::must_exists(i.id().to_owned()), i);
        }

        c
    }
}
