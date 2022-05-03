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

    /// private method to build an Id that exists in the [Collection]
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

// Implements Borrow to be able to look in the hashmap with just a &str instead of a String
impl<T> std::borrow::Borrow<str> for Id<T> {
    fn borrow(&self) -> &str {
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

    /// Get a typed [Id] from a raw &str
    /// An [Id] can be returned only if it exists in the [Collection]
    pub fn get_id(&self, raw_id: &str) -> Option<Id<T>> {
        self.get_by_str(raw_id).map(|(k, _v)| k.clone())
    }

    /// Get an &[Id] and a reference to the object associated with it if it exists in the [Collection]
    pub(crate) fn get_by_str(&self, raw_id: &str) -> Option<(&Id<T>, &T)> {
        self.0.get_key_value(raw_id)
    }
    
    /// Get an &[Id] and a mutable reference to the object associated with it if it exists in the [Collection]
    pub(crate) fn get_mut_by_str(&mut self, raw_id: &str) -> Option<(Id<T>, &mut T)> {
        self.0.get_mut(raw_id).map(|v| (Id::must_exists(raw_id.to_owned()), v))
    }

    /// Get the object associated to the typed [Id]
    pub fn get(&self, id: &Id<T>) -> Option<&T> {
        self.0.get(id)
    }

    /// Returns the number of objects in the [Collection]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

// Implements FromIterator to be able to easily build a [Collection] if we know how to associate an object with its [Id]
impl<T: WithId> FromIterator<T> for Collection<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut c = Self::default();

        for i in iter {
            let _ = c.0.insert(Id::must_exists(i.id().to_owned()), i);
        }

        c
    }
}
