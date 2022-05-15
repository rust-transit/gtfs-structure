use crate::WithId;
use core::marker::PhantomData;
use std::{
    collections::{hash_map, HashMap},
    iter::FromIterator,
};

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

    /// Extracts a string slice containing the entire [Id]
    pub fn as_str(&self) -> &str {
        self
    }
}

impl<T> std::convert::AsRef<str> for Id<T> {
    fn as_ref(&self) -> &str {
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
        self.0
            .get_mut(raw_id)
            .map(|v| (Id::must_exists(raw_id.to_owned()), v))
    }

    /// Get the object associated to the typed [Id]
    pub fn get(&self, id: &Id<T>) -> Option<&T> {
        self.0.get(id)
    }

    /// Returns the number of objects in the [Collection]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    // Return true if the collection has no objects.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over the ([Id]<T>, &T) of the [Collection].
    pub fn iter(&self) -> hash_map::Iter<Id<T>, T> {
        self.0.iter()
    }

    /// Iterates over the &T of the [Collection].
    pub fn values(&self) -> hash_map::Values<'_, Id<T>, T> {
        self.0.values()
    }

    /// Iterates over the &mut T of the [Collection].
    pub fn values_mut(&mut self) -> hash_map::ValuesMut<'_, Id<T>, T> {
        self.0.values_mut()
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

impl<'a, T> IntoIterator for &'a Collection<T> {
    type Item = (&'a Id<T>, &'a T);
    type IntoIter = hash_map::Iter<'a, Id<T>, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> IntoIterator for Collection<T> {
    type Item = (Id<T>, T);
    type IntoIter = hash_map::IntoIter<Id<T>, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
